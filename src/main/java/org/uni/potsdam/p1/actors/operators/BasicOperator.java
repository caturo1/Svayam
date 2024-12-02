package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.operators.cores.BasicOperatorCore;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

public class BasicOperator extends ProcessFunction<Measurement, Measurement> {

  BasicOperatorCore core;

  /**
   * Constructs a new processor using the given operator's information as well as the
   * specified batchSize for the {@link Measurer} instances used to calculate the processing
   * {@link Metrics}.
   *
   * @param operator Information about this operator.
   */
  public BasicOperator(OperatorInfo operator) {
    core = new BasicOperatorCore(operator);
  }

  /**
   * Set a new side output for one pattern implemented by this operator
   *
   * @param operatorName Name/Type of the operators to be referenced.
   * @param whereTo      Output channel where the events are to be forwarded to
   * @return A reference to this instance.
   */
  public BasicOperator setSideOutput(String operatorName, MeasurementOutput whereTo) {
    core.setSideOutput(operatorName, whereTo);
    return this;
  }


  /**
   * Set a new side output for one metric calculated by this operator
   *
   * @param metric  Name of the metric to be referenced
   * @param whereTo Output channel where the metrics are to be forwarded to
   * @return A reference to this instance.
   */
  public BasicOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
    core.setMetricsOutput(metric, whereTo);
    return this;
  }

  /**
   * Processes each Measurement event in all patterns of this operator, for which the
   * pattern-specific shedding rate is greater than a pseudo-random value. Measures and
   * updates the processing time as well as the processing and output rates.
   *
   * @param value The stream element
   * @param ctx   A {@link ProcessFunction.Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception Error in the flink's thread execution.
   */
  @Override
  public void processElement(Measurement value, ProcessFunction<Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {

    core.processWithContext(value, ctx);

  }
}
