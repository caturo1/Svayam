package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

/**
 * This class is used to measure the input rates of operators which have inputs of many
 * different sources.It must be connected to the analyser of its respective operator using
 * an {@link MetricsOutput}.It also logs the events seen for data analysis.
 * One has to set
 * the MetricsOutput channel for the inputRates of this class for it to
 * function properly. This can be done using the constructor or with the
 * {@link JoinCounter#setMetricsOutput(String, MetricsOutput)} method.
 */
public class JoinCounter extends ProcessFunction<Measurement, Measurement> {

  private final Logger joinLog = LoggerFactory.getLogger("joinLog");

  // define outputTags for the side-outputs
  MetricsOutput inputRates;

  // define the Measurers for the stream characteristics
  CountingMeasurer inputRateMeasurer;

  public JoinCounter(OperatorInfo operator, MetricsOutput toAnalyser) {
    inputRateMeasurer = new CountingMeasurer(operator.name, operator.inputTypes, "lambdaIn", operator.controlBatchSize);
    inputRates = toAnalyser;
  }

  public JoinCounter(OperatorInfo operator) {
    inputRateMeasurer = new CountingMeasurer(operator.name, operator.inputTypes, "lambdaIn", operator.controlBatchSize);
  }

  @Override
  public void processElement(Measurement value, Context ctx, Collector<Measurement> out) throws Exception {
    inputRateMeasurer.update(value.getTypeAsKey());
    out.collect(value);
    if (inputRateMeasurer.isReady() && inputRates != null) {
      ctx.output(inputRates, inputRateMeasurer.getNewestAverages());
    }
    joinLog.info(value.toJson());
  }

  /**
   * Set a new side output for one metric calculated by this operator
   *
   * @param metric  Name of the metric to be referenced
   * @param whereTo Output channel where the metrics are to be forwarded to
   * @return A reference to this instance.
   */
  public JoinCounter setMetricsOutput(String metric, MetricsOutput whereTo) {
    if (metric.equals("lambdaIn")) {
      inputRates = whereTo;
    }
    return this;
  }
}
