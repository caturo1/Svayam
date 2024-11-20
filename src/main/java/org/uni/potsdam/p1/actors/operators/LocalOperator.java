package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.actors.measurers.AddingMeasurer;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.processors.fsm.AndFSMProcessor;
import org.uni.potsdam.p1.actors.processors.fsm.FSMProcessor;
import org.uni.potsdam.p1.actors.processors.fsm.OrFSMProcessor;
import org.uni.potsdam.p1.actors.processors.fsm.SeqFSMProcessor;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Variant of the {@link FSMOperator} for local load shedding. This operator analyses its
 * own stream characteristics and calculates its own shedding configuration.
 */
public class LocalOperator extends ProcessFunction<Measurement, Measurement> {

  private final Logger opLog = LoggerFactory.getLogger("opLog");

  // define outputTags for the side-outputs
  MetricsOutput sosOutput;
  MetricsOutput outputRates;
  MetricsOutput processingTimes;
  MetricsOutput processingRates;
  Map<String, MeasurementOutput> extraOutputs;

  // define the Measurers for the stream characteristics
  CountingMeasurer outputRateMeasurer;
  AddingMeasurer processingTimesMeasurer;
  CountingMeasurer processingRateMeasurer;

  // define sheddingShares
  public Metrics sheddingRates;
  boolean isShedding = false;
  double factor;

  // set additional operator information
  OperatorInfo operator;
  Map<String, FSMProcessor> processors;

  /**
   * Constructs a new processor using the given operator's information as well as the
   * specified batchSize for the {@link Measurer} instances used to calculate the processing
   * {@link Metrics}.
   *
   * @param operator Information about this operator.
   */
  public LocalOperator(OperatorInfo operator) {

    // gather basic information
    String groupName = operator.name;
    String[] outputTypes = operator.outputTypes;
    String[] inputTypes = operator.inputTypes;
    int batchSize = operator.controlBatchSize;

    // initialise Measurer
    outputRateMeasurer = new CountingMeasurer(groupName, outputTypes, "lambdaOut", batchSize);
    processingRateMeasurer = new CountingMeasurer(groupName, inputTypes, "mu", batchSize);
    processingTimesMeasurer = new AddingMeasurer(groupName, outputTypes, "ptime", batchSize);

    // reference operator's info
    this.operator = operator;

    // initialise processors for each pattern
    processors = new HashMap<>(operator.patterns.length);
    for (EventPattern pattern : operator.patterns) {
      switch (pattern.getType()) {
        case "AND": {
          processors.put(pattern.name, new AndFSMProcessor(pattern));
          break;
        }
        case "OR": {
          processors.put(pattern.name, new OrFSMProcessor(pattern));
          break;
        }
        case "SEQ": {
          processors.put(pattern.name, new SeqFSMProcessor(pattern));
        }
      }
    }
    sheddingRates = new Metrics(operator.name, "shares", operator.outputTypes.length * operator.inputTypes.length + 1);
    for (String lambdaKey : operator.inputTypes) {
      for (String ptimeKey : operator.outputTypes) {
        sheddingRates.put(ptimeKey + "_" + lambdaKey, 0.);
      }
    }
  }

  /**
   * Set a new side output for one pattern implemented by this operator
   *
   * @param patternName Name/Type of the pattern to be referenced
   * @param whereTo     Output channel where the events are to be forwarded to
   * @return A reference to this instance.
   */
  public LocalOperator setSideOutput(String patternName, MeasurementOutput whereTo) {
    int size = operator.patterns.length;
    if (extraOutputs == null) {
      extraOutputs = new HashMap<>(size);
    }
    extraOutputs.put(patternName, whereTo);
    return this;
  }

  /**
   * Set a new side output for one metric calculated by this operator
   *
   * @param metric  Name of the metric to be referenced
   * @param whereTo Output channel where the metrics are to be forwarded to
   * @return A reference to this instance.
   */
  public LocalOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
    switch (metric) {
      case "lambdaOut": {
        outputRates = whereTo;
        break;
      }
      case "ptime": {
        processingTimes = whereTo;
        break;
      }
      case "mu": {
        processingRates = whereTo;
        break;
      }
      case "sos": {
        sosOutput = whereTo;
      }
    }
    return this;
  }

  /**
   * Processes each Measurement event in all patterns of this operator, for which the
   * pattern-specific shedding rate is greater than a pseudo-random value. Measures and
   * updates the processing time as well as the processing and output rates.
   *
   * @param value The stream element
   * @param ctx   A {@link KeyedCoProcessFunction.Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception Error in the flink's thread execution.
   */
  @Override
  public void processElement(Measurement value, ProcessFunction<Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {

    LocalTime begin = LocalTime.now();
    for (EventPattern pattern : operator.patterns) {
      boolean dropPattern = isShedding && sheddingRates.get(pattern.name + "_" + value.type) > Math.random();

      LocalTime start = LocalTime.now();

      if (!dropPattern) {
        Measurement measurement = processors.get(pattern.name).processElement(value);
        if (measurement != null) {
          outputRateMeasurer.update(pattern.name);
          if (extraOutputs != null && extraOutputs.containsKey(pattern.name)) {
            ctx.output(extraOutputs.get(pattern.name), measurement);
          } else {
            out.collect(measurement);
          }
          opLog.info(measurement.toJson(operator.name));
        }
      }
      processingTimesMeasurer.updatePatternTime(pattern.name, Duration.between(start, LocalTime.now()).toNanos());
    }
    processingTimesMeasurer.updateQueue(begin, LocalTime.now());
    processingRateMeasurer.update(value.getTypeAsKey());

    if (processingTimesMeasurer.isReady()) {
      updateAndForward(processingTimesMeasurer, processingTimes, ctx);
      updateAndForward(processingRateMeasurer, processingRates, ctx);

      double timeTaken = processingTimesMeasurer.getNewestAverages().get("total");
      opLog.info(String.format("{\"ptime\": %f, \"name\": \"%s\"}", timeTaken, operator.name));
      double upperBound = 1 / ((1 / operator.latencyBound) - processingRateMeasurer.getTotalAverageRate());
      double lowerBound = upperBound * 0.9;
      if (timeTaken > lowerBound) {
        isShedding = true;
        factor = timeTaken / lowerBound;
        calculateSheddingRate();
      } else {
        isShedding = false;
      }
    }

    if (outputRateMeasurer.isReady()) {
      updateAndForward(outputRateMeasurer, outputRates, ctx);
    }

  }

  /**
   * <p>
   * Calculate the next shedding rates to be used by this operator. Shedding rates are
   * calculated for each event type in each pattern. The proportions to share for a single
   * event type are determined by the amount of events of this type that are processed
   * in average per second, the total relevance of the corresponding pattern in all of
   * an operator's output and the amount of events of this type in a pattern. We calculate:
   * </p>
   * <p>
   * ShedRate_EventX_PatternY =
   * <br>
   * (processingRateOfX / totalProcessingRateOfEventsInY) *
   * <br>
   * (1 / amountOfEventsOfTypeXinPatternY) *
   * <br>
   * (outputRatesOfY / totalOutputRates)
   * </p>
   */
  public void calculateSheddingRate() {
    Metrics mus = processingRateMeasurer.getLatestAverages();
//    Metrics ptimes = processingTimesMeasurer.getLatestAverages();
    Metrics lambdaOuts = outputRateMeasurer.getLatestAverages();
    for (EventPattern eventPattern : operator.patterns) {
      Map<String, Integer> weights = eventPattern.getWeightMaps();
//      double minimum = mus.getWeightedMinimum(weights);
      double sum = 0.;
      for (String types : weights.keySet()) {
        sum += mus.get(types);
      }
      String patternKey = eventPattern.name;
      for (String inputType : operator.inputTypes) {
        double value;
        if (!weights.containsKey(inputType)) {
          value = 1;
        } else {
          value = Math.min(1,
            factor * (mus.get(inputType) / (weights.get(inputType) * sum)) * (lambdaOuts.get(patternKey) / lambdaOuts.get("total")));// * (ptimes.get(patternKey) / ptimes.get("total")));
        }
        sheddingRates.put(eventPattern.name + "_" + inputType, value);
      }
    }
  }

  /**
   * Forwards the most recently calculated {@link Metrics} of a given type if it is
   * available.
   *
   * @param measurer      The measurer of this metric.
   * @param metricsOutput The side output to be used.
   * @param ctx           The context of this operator's ProcessFunction
   */
  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    if (metricsOutput != null) {
      ctx.output(metricsOutput, currentMetrics);
    }
  }
}
