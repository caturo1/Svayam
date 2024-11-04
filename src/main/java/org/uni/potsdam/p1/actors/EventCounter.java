package org.uni.potsdam.p1.actors;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.CountingMeasurer;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

/**
 * This class is used to measure the output rates of sources. It must be connected to the
 * analyser of its respective operator using an {@link OutputTag}.
 */
public class EventCounter extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  OutputTag<Metrics> outputRates;
  OutputTag<Metrics> sosOutput;
  CountingMeasurer outputRateMeasurer;

  public EventCounter(String groupName, String[] keys, OutputTag<Metrics> output, OutputTag<Metrics> sosOutput, int batchsize) {
    outputRateMeasurer = new CountingMeasurer(groupName, keys, "lambdaIn", batchsize);
    outputRates = output;
    this.sosOutput = sosOutput;
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    outputRateMeasurer.update(value);
    out.collect(value);
    if (outputRateMeasurer.isReady()) {
      ctx.output(outputRates, outputRateMeasurer.calculateNewestAverage());
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {
      String sosMessageId = value.substring(index + 1);
      ctx.output(sosOutput, outputRateMeasurer.getMetricsWithId(sosMessageId));
    }
  }
}
