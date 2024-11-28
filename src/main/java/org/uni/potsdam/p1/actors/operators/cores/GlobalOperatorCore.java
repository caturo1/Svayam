package org.uni.potsdam.p1.actors.operators.cores;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

/**
 * An {@link OperatorCore} to be used in execution environments with global {@link org.uni.potsdam.p1.execution.Settings}.
 * This class expands the basic core by specifying it to work with a {@link KeyedCoProcessFunction}'s
 * execution-context.
 */
public class GlobalOperatorCore extends OperatorCore {

  /**
   * Creates a new operator core.
   *
   * @param operator Operator's information used to create a new core.
   */
  public GlobalOperatorCore(OperatorInfo operator) {
    super(operator);
  }

  CoProcessFunction<Measurement, String, Measurement>.Context ctx;

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
      opLog.info(String.format("{\"ptime\": %f, \"time\": %d, \"name\": \"%s\"}", processingTimesMeasurer.getNewestAverages().get("total"), System.currentTimeMillis(), operator.name));
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
  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {
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
  public void processWithContext(Measurement value, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {
    this.ctx = ctx;
    super.process(value);
  }

  /**
   * Sends the newest calculated metrics to the coordinator if this operator just received
   * a sos-message (snap) or updates the operator's own shedding rates if the message receive
   * contains this operator's name.
   * Informs this operator's analyser that shedding has been activated/deactivated.
   *
   * @param value The stream element
   * @param ctx   A {@link KeyedCoProcessFunction.Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   */
  public void processMessages(String value, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {

    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {

      ctx.output(sosOutput, outputRateMeasurer.getMetrics());
      ctx.output(sosOutput, processingRateMeasurer.getMetrics());
      ctx.output(sosOutput, processingTimesMeasurer.getMetrics());

    } else if (message.equals(operator.name)) {
      boolean sharesAreAllZero = true;
      for (String share : value.substring(index + 1).split(":")) {
        int separationIndex = share.indexOf("|");
        String currentShare = share.substring(separationIndex + 1);
        if (sharesAreAllZero && !currentShare.equals("0.0")) {
          sharesAreAllZero = false;
        }
        sheddingRates.put(share.substring(0, separationIndex), Double.valueOf(currentShare));
      }
      boolean informAnalyser = !sharesAreAllZero || isShedding;
      if (isShedding && sharesAreAllZero) {
        isShedding = false;
        sheddingRates.put("shedding", Double.NEGATIVE_INFINITY);
        opLog.info(operator.getSheddingInfo(isShedding));
      } else if (!isShedding) {
        isShedding = true;
        sheddingRates.put("shedding", Double.POSITIVE_INFINITY);
        opLog.info(operator.getSheddingInfo(isShedding));
      }
      if (informAnalyser) {
        ctx.output(processingTimes, sheddingRates);
      }
    }
  }
}
