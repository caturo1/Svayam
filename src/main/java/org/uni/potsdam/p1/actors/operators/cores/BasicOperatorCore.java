package org.uni.potsdam.p1.actors.operators.cores;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

public class BasicOperatorCore extends OperatorCore {

  /**
   * Creates a new operator core.
   *
   * @param operator Operator's information used to create a new core.
   */
  public BasicOperatorCore(OperatorInfo operator) {
    super(operator);
  }

  ProcessFunction<Measurement, Measurement>.Context ctx;

  @Override
  protected void processSideOutputs(EventPattern pattern, Measurement value) {
    for (String downstreamOp : pattern.downstreamOperators) {
      ctx.output(extraOutputs.get(downstreamOp), value);
    }
  }

  @Override
  protected void processMeasuredRates() {
    if (processingTimesMeasurer.isReady()) {
      updateAndForward(processingTimesMeasurer, processingTimes, ctx);
      updateAndForward(processingRateMeasurer, processingRates, ctx);
      opLog.info(String.format("{\"ptime\": %f, \"time\": %d, \"name\": \"%s\"}", processingTimesMeasurer.results.get("total"), System.currentTimeMillis(), operator.name));
    }
    if (outputRateMeasurer.isReady()) {
      updateAndForward(outputRateMeasurer, outputRates, ctx);
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
  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, ProcessFunction<Measurement, Measurement>.Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    if (metricsOutput != null) {
      ctx.output(metricsOutput, currentMetrics);
    }
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
   */
  public void processWithContext(Measurement value, ProcessFunction<Measurement, Measurement>.Context ctx) {
    this.ctx = ctx;
    super.process(value);
  }
}
