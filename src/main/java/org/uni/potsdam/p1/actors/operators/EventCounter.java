package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

/**
 * This class is used to measure the input rates of the operators directly connected to
 * the sources. It must be connected to the analyser of its respective operator using
 * an {@link OutputTag}.
 */
public class EventCounter extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  // define outputTags for the side-outputs
  OutputTag<Metrics> outputRates;
  OutputTag<Metrics> sosOutput;

  // define the Measurers for the stream characteristics
  CountingMeasurer inputRateMeasurer;

  /**
   * Initialise the event counter.
   *
   * @param groupName  The operator's name
   * @param inputTypes The event types that the operator consume
   * @param output     A side output for forwarding the output rates to an analyser
   * @param sosOutput  A side output for forwarding the metrics gathered to the coordinator
   * @param batchSize  The amount of events needed to compute a running average of the metrics
   */
  public EventCounter(String groupName, String[] inputTypes, OutputTag<Metrics> output, OutputTag<Metrics> sosOutput, int batchSize) {
    inputRateMeasurer = new CountingMeasurer(groupName, inputTypes, "lambdaIn", batchSize);
    outputRates = output;
    this.sosOutput = sosOutput;
  }


  // measure output rates; forward source events to the operator
  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    inputRateMeasurer.update(value.getTypeAsKey());
    out.collect(value);
    if (inputRateMeasurer.isReady()) {
      ctx.output(outputRates, inputRateMeasurer.getNewestAverages());
    }
  }

  // send output rates to coordinator if a sos-message is received from the kafka channel
  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {
      String sosMessageId = value.substring(index + 1);
      ctx.output(sosOutput, inputRateMeasurer.getMetricsWithId(sosMessageId));
    }
  }
}
