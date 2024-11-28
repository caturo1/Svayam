package org.uni.potsdam.p1.actors.operators.cores;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.util.Map;

/**
 * An {@link OperatorCore} to be used in execution environments with local {@link org.uni.potsdam.p1.execution.Settings}.
 * This class expands the basic core by specifying it to work with a {@link ProcessFunction}'s
 * execution-context.
 */
public class LocalOperatorCore extends OperatorCore {

  public double factor;
  double lastLambda = 0.;
  double lastPtime = 0.;
  double lastAverage = 0.;

  /**
   * Creates a new operator core.
   *
   * @param operator Operator's information used to create a new core.
   */
  public LocalOperatorCore(OperatorInfo operator) {
    super(operator);
  }

  ProcessFunction<Measurement, Measurement>.Context ctx;

  @Override
  protected void processSideOutputs(EventPattern pattern, Measurement value) {
    for (String downstreamOp : pattern.downstreamOperators) {
      ctx.output(extraOutputs.get(downstreamOp), value);
    }
  }

  /**
   * <p>
   * Proofs if the measurers are ready to calculate their newest averages (minimum amount
   * of events for a running average is reached) and, if so, process them and log the
   * current total average processing time.
   * </p>
   * <p>
   * Evaluates the necessity of load shedding given the last calculate processing times
   * and, if shedding is needed or not needed anymore, updates the shedding rates of this operator.
   * </p>
   */
  @Override
  protected void processMeasuredRates() {
    if (processingTimesMeasurer.isReady()) {
      updateAndForward(processingTimesMeasurer, processingTimes, ctx);
      updateAndForward(processingRateMeasurer, processingRates, ctx);

      Metrics lambdaIn = processingRateMeasurer.results;
      Metrics ptime = processingTimesMeasurer.results;
      double total = lambdaIn.get("total");
      double calculatedP = 0.;
      for (String key : operator.inputTypes) {
        double weight = 0;
        for (String key2 : operator.outputTypes) {
          double share = (1 - sheddingRates.get(key2 + "_" + key));
          weight += share * ptime.get(key2);
        }
        calculatedP += (lambdaIn.get(key) / (total == 0 ? 1 : total)) * weight;
      }

      opLog.info(String.format("{\"ptime\": %f, \"time\": %d, \"name\": \"%s\"}", processingTimesMeasurer.getNewestAverages().get("total"), System.currentTimeMillis(), operator.name));

      double B = (1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total));
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      double ratioLambda = Math.abs(1 - lambdaIn.get("total") / (lastLambda == 0 ? 1 : lastLambda));
      double ratioPtime = Math.abs(1 - ptime.get("total") / (lastPtime == 0 ? 1 : lastPtime));
      double bound = operator.latencyBound;
      if (lastAverage != 0.) {
        if ((calculatedP > bound || (ratio > 0.1 || ratioLambda > 0.05 || ratioPtime > 0.05) && B > bound) || (isShedding && B < bound)) {
          if (!isShedding) {
            isShedding = true;
            opLog.info(operator.getSheddingInfo(isShedding));
          }
          double timeTaken = processingTimesMeasurer.getNewestAverages().get("total");
          double upperBound = 1 / ((1 / operator.latencyBound) + processingRateMeasurer.getTotalAverageRate());
          double lowerBound = upperBound * 0.9;
          factor = timeTaken / lowerBound;
          calculateSheddingRate();
        } else if (isShedding) {
          isShedding = false;
          opLog.info(operator.getSheddingInfo(isShedding));
        }
      }
      lastAverage = calculatedP;
      lastPtime = ptime.get("total");
      lastLambda = total;

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

  /**
   * <p>
   * Calculate the next shedding rates to be used by this operator. Shedding rates are
   * calculated for each event type in each pattern.
   * </p>
   * <p>
   * The proportions to share for a single
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
    Metrics lambdaOuts = outputRateMeasurer.getLatestAverages();
    for (EventPattern eventPattern : operator.patterns) {
      Map<String, Integer> weights = eventPattern.getWeightMaps();
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
}
