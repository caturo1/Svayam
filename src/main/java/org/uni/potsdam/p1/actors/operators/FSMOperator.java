package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
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
 * <p>
 * This class serves as the basic operator standard for processing complex events.
 * A FSMOperator can detect multiple event patterns and have multiple outputs. It
 * processes events using a {@link FSMProcessor} for each pattern and records basic
 * {@link Metrics} about the quality of its pattern-detection-process like:
 * </p>
 * <ul>
 *     <li>output rates = lambdaOut</li>
 *     <li>processing rates = mu</li>
 *     <li>processing time pro parameters = ptime</li>
 * </ul>
 * <p>
 * Instances of this class should receive a connected keyed stream of Measurements and Strings.
 * The Measurement-stream for the events to be processed and the String-stream for
 * Coordinator-messages.
 * </p>
 *
 * @see <a href=https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#keyed-streams">https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#keyed-streams</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#connected-streams">https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#connected-streams</a>
 * @see org.uni.potsdam.p1.actors.enrichers.Coordinator
 */
public class FSMOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

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
  Metrics sheddingShares;

  // set additional operator information
  OperatorInfo operator;
  boolean isShedding = false;
  Map<String, FSMProcessor> processors;

  /**
   * Constructs a new processor using the given operator's information as well as the
   * specified batchSize for the {@link Measurer} instances used to calculate the processing
   * {@link Metrics}.
   *
   * @param operator Information about this operator.
   */
  public FSMOperator(OperatorInfo operator) {

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

    // initialise shedding shares
    sheddingShares = new Metrics(groupName, "shares", inputTypes.length * outputTypes.length + 1);
    for (String inputType : inputTypes) {
      for (String outType : outputTypes) {
        sheddingShares.put(outType + "_" + inputType, 0.);
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
  public FSMOperator setSideOutput(String patternName, MeasurementOutput whereTo) {
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
  public FSMOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
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
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception
   */
  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {

    double prob = Math.random();

    LocalTime begin = LocalTime.now();
    for (EventPattern pattern : operator.patterns) {
      boolean dropPattern = isShedding && sheddingShares.get(pattern.name + "_" + value.type) > prob;

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
        }
      }
      processingTimesMeasurer.updatePatternTime(pattern.name, Duration.between(start, LocalTime.now()).toNanos());
    }
    processingTimesMeasurer.updateQueue(begin, LocalTime.now());
    processingRateMeasurer.update(value.getTypeAsKey());

    if (processingTimesMeasurer.isReady()) {
      updateAndForward(processingTimesMeasurer, processingTimes, ctx);
      updateAndForward(processingRateMeasurer, processingRates, ctx);
    }

    if (outputRateMeasurer.isReady()) {
      updateAndForward(outputRateMeasurer, outputRates, ctx);
    }
  }

  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    if (metricsOutput != null) {
      ctx.output(metricsOutput, currentMetrics);
    }
  }

  /**
   * Sends the newest calculated metrics to coordinator if it receives a sos-message (snap)
   * or updates its own shedding rates if the message receive contains this operator's name.
   * Informs this operator's analyser that shedding has been activated/deactivated.
   *
   * @param value The stream element
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception
   */
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

    } else if (message.equals(operator.name)) {
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
