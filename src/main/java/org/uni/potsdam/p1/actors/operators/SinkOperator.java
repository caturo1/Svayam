package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.measurers.AddingMeasurer;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.actors.processors.liststate.SinkPatternProcessor;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Optional;

/**
 * This class represents the last operators which are connected to the sinks. Its instances
 * work similar to those of {@link BasicOperator}. Implements a simple AND pattern with
 * the events it receives from two different operators without time constraints.
 */
public class SinkOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  // define outputTags for the side-outputs
  OutputTag<Metrics> sosOutput;
  OutputTag<Metrics> processingTimes;

  // define the Measurers for the stream characteristics
  CountingMeasurer outputRateMeasurer;
  AddingMeasurer processingTimesMeasurer;
  CountingMeasurer processingRateMeasurer;

  // define ListStates for pattern detection (one pro relevant input type)
  ListState<Measurement> zero;
  ListState<Measurement> first;

  // define sheddingShares
  Metrics sheddingShares;

  // set additional operator information
  String groupName;
  String outputType;
  boolean isShedding;

  public SinkOperator(String groupName, String[] inputTypes, String[] outputTypes, OutputTag<Metrics> processingTimes, OutputTag<Metrics> sosOutput, int batchSize) {

    outputRateMeasurer = new CountingMeasurer(groupName, outputTypes, "lambdaOut", batchSize);
    processingRateMeasurer = new CountingMeasurer(groupName, inputTypes, "mu", batchSize);
    processingTimesMeasurer = new AddingMeasurer(groupName, outputTypes, "ptime", batchSize);

    this.sosOutput = sosOutput;
    this.processingTimes = processingTimes;

    sheddingShares = new Metrics(groupName, "shares", inputTypes.length * outputTypes.length + 1);
    for (String inputType : inputTypes) {
      for (String outType : outputTypes) {
        sheddingShares.put(outType + "_" + inputType, 0.);
      }
    }

    outputType = outputTypes[0];
  }

  // activate ListStates
  @Override
  public void open(OpenContext openContext) throws Exception {
    zero = getRuntimeContext().getListState(new ListStateDescriptor<>("zero", Measurement.class));
    first = getRuntimeContext().getListState(new ListStateDescriptor<>("first", Measurement.class));
  }

  // execute pattern detection here
  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int type = value.type;
    double prob = Math.random();
    boolean drop = isShedding && sheddingShares.get(outputType + "_" + value.type) > prob;
    LocalTime begin = LocalTime.now();
    Optional<Measurement> result = Optional.empty();
    if (!drop) {
      if ((type == 11 || type == 12)) {
        result = new SinkPatternProcessor(value, outputType, zero, first, outputRateMeasurer).process();
      } else {
        result = new SinkPatternProcessor(value, outputType, first, zero, outputRateMeasurer).process();
      }
      result.ifPresent(out::collect);
    }
    LocalTime end = LocalTime.now();

    // collect processing times
    long timePattern = Duration.between(begin, end).toNanos();
    processingTimesMeasurer.updatePatternTime(outputType, timePattern);
    processingTimesMeasurer.updateQueue(begin, end);

    // update processing rates
    processingRateMeasurer.update(value.getTypeAsKey());

    if (processingTimesMeasurer.isReady()) {
      ctx.output(processingTimes, processingTimesMeasurer.getNewestAverages());
      processingRateMeasurer.getNewestAverages();
    }

    if (outputRateMeasurer.isReady()) {
      outputRateMeasurer.getNewestAverages();
    }
  }

  // send output rates to coordinator if a sos-message is received from the kafka channel
  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {
      String sosMessageId = value.substring(index + 1);

      //TODO send load shares and all metrics together
      ctx.output(sosOutput, outputRateMeasurer.getMetricsWithId(sosMessageId));
      ctx.output(sosOutput, processingRateMeasurer.getMetricsWithId(sosMessageId));
      ctx.output(sosOutput, processingTimesMeasurer.getMetricsWithId(sosMessageId));

    } else if (message.equals(groupName)) {
      boolean sharesAreAllZero = true;
      for (String share : value.substring(index + 1).split(":")) {
        int separationIndex = share.indexOf("|");
        String currentShare = share.substring(separationIndex + 1);
        if (sharesAreAllZero && !currentShare.equals("0.0")) {
          sharesAreAllZero = false;
        }
        sheddingShares.put(share.substring(0, separationIndex), Double.valueOf(currentShare));
      }
      boolean informAnalyser = !sharesAreAllZero || isShedding;
      if (sharesAreAllZero) {
        isShedding = false;
        sheddingShares.put("shedding", Double.NEGATIVE_INFINITY);
      } else if (!isShedding) {
        isShedding = true;
        sheddingShares.put("shedding", Double.POSITIVE_INFINITY);
      }
      sheddingShares.put("batch", (double) processingTimesMeasurer.batch);
      if (informAnalyser) {
        ctx.output(processingTimes, sheddingShares);
      }
    }
  }
}
