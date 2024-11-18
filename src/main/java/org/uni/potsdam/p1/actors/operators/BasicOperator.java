package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.measurers.AddingMeasurer;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.processors.liststate.AndPatternProcessor;
import org.uni.potsdam.p1.actors.processors.liststate.PatternProcessor;
import org.uni.potsdam.p1.actors.processors.liststate.SeqPatternProcessor;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Optional;

/**
 * <p>
 * This class implements a complex processing operator and serves as an
 * implementation standard. It describes an operator which has two outputs, that
 * receives four types of events and that can request and utilise different load
 * shedding configurations at runtime.
 * </p>
 * <p>
 * Pattern detection is done using list states
 * (lists of events of a certain time order by time of arrival at the operator), whereby
 * the processing method utilized should be defined in a {@link PatternProcessor} class,
 * which is then applied here.
 * </p>
 * <p>
 * The operator produces complex {@link Measurement} events and uses {@link Measurer}
 * instances to calculate its averaged output and processing rates as well as the
 * average processing time of each pattern.
 * </p>
 */
@Deprecated
public class BasicOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  // define outputTags for the side-outputs
  OutputTag<Metrics> outputRates;
  OutputTag<Metrics> sosOutput;
  OutputTag<Metrics> processingTimes;
  OutputTag<Measurement> secondOutput;

  // define the Measurers for the stream characteristics
  CountingMeasurer outputRateMeasurer;
  AddingMeasurer processingTimesMeasurer;
  CountingMeasurer processingRateMeasurer;

  // define ListStates for pattern detection (one pro relevant input type)
  ListState<Measurement> zero;
  ListState<Measurement> first;
  ListState<Measurement> second;
  ListState<Measurement> third;

  // define sheddingShares
  Metrics sheddingShares;

  // set additional operator information
  String groupName;
  String[] outputTypes;
  boolean isShedding = false;

  /**
   * Initialise a standard operator
   *
   * @param groupName       The operator's name
   * @param inputTypes      The event types that the operator consumes
   * @param outputTypes     The types given to complex events detected by this operator
   * @param secondOutput    The second output in which the operator can forward its complex events
   * @param processingTimes A side output for forwarding the processing times of its patterns to an analyser
   * @param outputRates     A side output for forwarding the output rates to an analyser
   * @param sosOutput       A side output for forwarding the metrics gathered to the coordinator
   * @param batchSize       The amount of events needed to compute a running average of the metrics
   */
  public BasicOperator(String groupName, String[] inputTypes, String[] outputTypes, OutputTag<Measurement> secondOutput, OutputTag<Metrics> processingTimes, OutputTag<Metrics> outputRates, OutputTag<Metrics> sosOutput, int batchSize) {

    outputRateMeasurer = new CountingMeasurer(groupName, outputTypes, "lambdaOut", batchSize);
    processingRateMeasurer = new CountingMeasurer(groupName, inputTypes, "mu", batchSize);
    processingTimesMeasurer = new AddingMeasurer(groupName, outputTypes, "ptime", batchSize);

    this.outputTypes = outputTypes;
    this.outputRates = outputRates;
    this.sosOutput = sosOutput;
    this.secondOutput = secondOutput;
    this.processingTimes = processingTimes;

    sheddingShares = new Metrics(groupName, "shares", inputTypes.length * outputTypes.length + 1);
    for (String inputType : inputTypes) {
      for (String outType : outputTypes) {
        sheddingShares.put(outType + "_" + inputType, 0.);
      }
    }
  }

  // activate ListStates
  @Override
  public void open(OpenContext openContext) throws Exception {
    zero = getRuntimeContext().getListState(new ListStateDescriptor<>("zero", Measurement.class));
    first = getRuntimeContext().getListState(new ListStateDescriptor<>("first", Measurement.class));
    second = getRuntimeContext().getListState(new ListStateDescriptor<>("second", Measurement.class));
    third = getRuntimeContext().getListState(new ListStateDescriptor<>("third", Measurement.class));
  }

  // execute pattern detection here
  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {

    double prob = Math.random();
    String outputType1 = outputTypes[0];
    String outputType2 = outputTypes[1];

    boolean dropPattern1 = isShedding && sheddingShares.get(outputType1 + "_" + value.type) > prob;
    boolean dropPattern2 = isShedding && sheddingShares.get(outputType2 + "_" + value.type) > prob;

    LocalTime begin = LocalTime.now();

    if (!dropPattern1) {
      new SeqPatternProcessor(value, outputType1, zero, outputRateMeasurer)
        .process().ifPresent(out::collect);
    }

    LocalTime middle = LocalTime.now();

    if (!dropPattern2) {
      if (value.type != 0) {
        Optional<Measurement> result = Optional.empty();
        switch (value.type) {
          case 1: {
            result = new AndPatternProcessor(value, outputType2, first, second, third, outputRateMeasurer).process();
            break;
          }
          case 2: {
            result = new AndPatternProcessor(value, outputType2, second, first, third, outputRateMeasurer).process();
            break;
          }
          case 3: {
            result = new AndPatternProcessor(value, outputType2, third, first, second, outputRateMeasurer).process();
          }
        }
        result.ifPresent(measurement -> ctx.output(secondOutput, measurement));
      }
    }

    LocalTime end = LocalTime.now();

    // calculate processing duration of each pattern
    long timePattern1 = Duration.between(begin, middle).toNanos();
    long timePattern2 = Duration.between(middle, end).toNanos();

    // update processing times
    processingTimesMeasurer.updatePatternTime(outputType1, timePattern1);
    processingTimesMeasurer.updatePatternTime(outputType2, timePattern2);
    processingTimesMeasurer.updateQueue(begin, end);

    // update processing rates
    processingRateMeasurer.update(value.getTypeAsKey());

    if (processingTimesMeasurer.isReady()) {
      ctx.output(processingTimes, processingTimesMeasurer.getNewestAverages());
      processingRateMeasurer.getNewestAverages();
    }

    if (outputRateMeasurer.isReady()) {
      ctx.output(outputRates, outputRateMeasurer.getNewestAverages());
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
