package org.uni.potsdam.p1.actors.operators.cores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.actors.measurers.AddingMeasurer;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.actors.processors.FSMProcessor;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * This class contains the basic implementation of an operator, setting its measurers,
 * logging components, side outputs, shedding shares and cep-processors as well as defining
 * the core processing logic to be used inside either a {@link org.uni.potsdam.p1.actors.operators.FSMOperator}
 * (global scope) or a {@link org.uni.potsdam.p1.actors.operators.LocalOperator} (local
 * scope).
 *
 * @see GlobalOperatorCore
 * @see LocalOperatorCore
 */
public abstract class OperatorCore implements Serializable {
  /**
   * Empty constructor for flink.
   */
  public OperatorCore() {
  }

  // define outputTags for the side-outputs
  public MetricsOutput sosOutput;
  public MetricsOutput outputRates;
  public MetricsOutput processingTimes;
  public MetricsOutput processingRates;
  public Map<String, EventOutput> extraOutputs;

  public Logger opLog = LoggerFactory.getLogger("opLog");

  // define the Measurers for the stream characteristics
  public CountingMeasurer outputRateMeasurer;
  public AddingMeasurer processingTimesMeasurer;
  public CountingMeasurer processingRateMeasurer;

  // define sheddingRates
  public Metrics sheddingRates;
  public boolean isShedding = false;

  // set additional operator information
  public OperatorInfo operator;
  public Map<String, FSMProcessor> processors;

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    OperatorCore that = (OperatorCore) o;
    return isShedding == that.isShedding && Objects.equals(sosOutput, that.sosOutput) && Objects.equals(outputRates, that.outputRates) && Objects.equals(processingTimes, that.processingTimes) && Objects.equals(processingRates, that.processingRates) && Objects.equals(extraOutputs, that.extraOutputs) && Objects.equals(opLog, that.opLog) && Objects.equals(outputRateMeasurer, that.outputRateMeasurer) && Objects.equals(processingTimesMeasurer, that.processingTimesMeasurer) && Objects.equals(processingRateMeasurer, that.processingRateMeasurer) && Objects.equals(sheddingRates, that.sheddingRates) && Objects.equals(operator, that.operator) && Objects.equals(processors, that.processors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sosOutput, outputRates, processingTimes, processingRates, extraOutputs, opLog, outputRateMeasurer, processingTimesMeasurer, processingRateMeasurer, sheddingRates, isShedding, operator, processors);
  }

  /**
   * Creates a new operator core.
   *
   * @param operator Operator's information used to create a new core.
   */
  public OperatorCore(OperatorInfo operator) {
    this.operator = operator;
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
    EventPattern.addProcessors(processors, operator.patterns);

    // initialise shedding shares
    sheddingRates = new Metrics(groupName, "shares", inputTypes.length * outputTypes.length + 1);
    for (String inputType : inputTypes) {
      for (String outType : outputTypes) {
        sheddingRates.put(outType + "_" + inputType, 0.);
      }
    }
  }

  /**
   * Set a new side output for one pattern implemented by this operator
   *
   * @param operatorName Name/Type of the operators to be referenced.
   * @param whereTo      Output channel where the events are to be forwarded to
   */
  public void setSideOutput(String operatorName, EventOutput whereTo) {
    int size = operator.patterns.length;
    if (extraOutputs == null) {
      extraOutputs = new HashMap<>(size);
    }
    extraOutputs.put(operatorName, whereTo);
  }

  /**
   * Set a new side output for one metric calculated by this operator
   *
   * @param metric  Name of the metric to be referenced
   * @param whereTo Output channel where the metrics are to be forwarded to
   */
  public void setMetricsOutput(String metric, MetricsOutput whereTo) {
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
  }

  /**
   * Process a new {@link Event}, updates the measurers values and possibly
   * produces an output to be forwarded to a specified operator downstream.
   *
   * @param value Latest read event.
   */
  public void process(Event value) {
    LocalTime begin = LocalTime.now();
    for (EventPattern pattern : operator.patterns) {
      boolean dropPattern = isShedding && sheddingRates.get(pattern.name + "_" + value.type) > Math.random();

      LocalTime start = LocalTime.now();

      if (!dropPattern) {
        Event event = processors.get(pattern.name).processElement(value);
        if (event != null) {
          outputRateMeasurer.update(pattern.name);
          processSideOutputs(pattern, event);
          opLog.info(event.toJson(operator.name));
        }
      }
      processingTimesMeasurer.updatePatternTime(pattern.name, Duration.between(start, LocalTime.now()).toNanos());
    }
    processingTimesMeasurer.updateQueue(begin, LocalTime.now());
    processingRateMeasurer.update(value.getTypeAsKey());

    processMeasuredRates();
  }

  /**
   * Sends a newly produced event of one of this operator's patterns to all specified operators
   * downstream.
   *
   * @param pattern The pattern that got matched.
   * @param value   The newly created event of this pattern's name to be forwarded downstream.
   */
  protected abstract void processSideOutputs(EventPattern pattern, Event value);

  /**
   * Proofs if the measurers are ready to calculate their newest averages (minimum amount
   * of events for a running average is reached) and, if so, process them and log the
   * current total average processing time.
   */
  protected abstract void processMeasuredRates();

}
