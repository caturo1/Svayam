package org.uni.potsdam.p1.actors.operators.tools;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

/**
 * This class is used to measure the input rates of the operators directly connected to
 * the sources. It must be connected to the analyser of its respective operator using
 * an {@link OutputTag}. It also logs the events seen for data analysis.
 * One has to set
 * the MetricsOutput channels for the inputRates and sosOutput of this class for it to
 * function properly. This can be done using the constructor or with the
 * {@link SourceCounter#setMetricsOutput(String, MetricsOutput)} method.
 */
public class SourceCounter extends CoProcessFunction<Measurement, String, Measurement> {

  // define outputTags for the side-outputs
  MetricsOutput inputRates;
  MetricsOutput sosOutput;

  // define the Measurers for the stream characteristics
  CountingMeasurer inputRateMeasurer;
  String name;
  String sourceName;

  /**
   * Initialise the event counter.
   *
   * @param operator  The operator's information
   * @param output    A side output for forwarding the output rates to an analyser
   * @param sosOutput A side output for forwarding the metrics gathered to the coordinator
   */
  public SourceCounter(OperatorInfo operator, MetricsOutput output, MetricsOutput sosOutput) {
    inputRateMeasurer = new CountingMeasurer(operator.name, operator.inputTypes, "lambdaIn", operator.controlBatchSize);
    inputRates = output;
    this.sosOutput = sosOutput;
    this.name = operator.name;
  }

  /**
   * Initialise the event counter.
   *
   * @param operator The operator's information
   */
  public SourceCounter(OperatorInfo operator) {
    inputRateMeasurer = new CountingMeasurer(operator.name, operator.inputTypes, "lambdaIn", operator.controlBatchSize);
    this.name = operator.name;
  }

  /**
   * Set a new side output for one metric calculated by this operator
   *
   * @param metric  Name of the metric to be referenced
   * @param whereTo Output channel where the metrics are to be forwarded to
   * @return A reference to this instance.
   */
  public SourceCounter setMetricsOutput(String metric, MetricsOutput whereTo) {
    switch (metric) {
      case "lambdaIn": {
        inputRates = whereTo;
        break;
      }
      case "sos": {
        sosOutput = whereTo;
      }
    }
    return this;
  }

  /**
   * Measure output rates; forward source events to the operator
   *
   * @param value The stream element
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception Flink's exception happens
   */
  @Override
  public void processElement1(Measurement value, CoProcessFunction<Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    inputRateMeasurer.update(value.getTypeAsKey());
    out.collect(value);
    if (inputRateMeasurer.isReady()) {
      ctx.output(inputRates, inputRateMeasurer.getNewestAverages());
    }
  }

  /**
   * Send output rates to coordinator if a sos-message is received from the kafka channel
   *
   * @param value The stream element
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception Flink's exception happens
   */
  @Override
  public void processElement2(String value, CoProcessFunction<Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {
      ctx.output(sosOutput, inputRateMeasurer.getMetrics());
    }
  }
}
