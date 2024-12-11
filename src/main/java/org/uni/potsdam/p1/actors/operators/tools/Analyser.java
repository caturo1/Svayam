package org.uni.potsdam.p1.actors.operators.tools;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

/**
 * This class analyses the stream characteristics of an operator. It consumes the
 * operator's input rates, processing times per pattern and its shedding shares to
 * evaluate the necessity of load shedding. If an overload is detected this class
 * will forward an empty {@link Metrics} metrics with "overloaded" description, which
 * activates the coordinator. This class can be used for both local or global load
 * shedding. In the latter case its output must be sent to a {@link Coordinator} whereas
 * in the first case it suffices to connect the output of an instance of this class with
 * an operator.
 */
public class Analyser extends ProcessFunction<Metrics, Metrics> {

  // define latest metrics received
  Metrics ptime;
  long arrivalTimePtime = -1;
  long deltaX = 0;

  Metrics lambdaIn;
  long arrivalTimeLambda = -1;

  // define operator's information
  OperatorInfo operator;

  // define shedding information
  Metrics sheddingRates;
  boolean isShedding = false;
  boolean isCoordinatorInformed = false;

  // store last averages
  double lastLambda;
  double lastPtime;
  double lastAverage;

  public StringOutput toKafka;

//  public Logger analyserLog = LoggerFactory.getLogger("analyserLog");

  /**
   * Constructs a new Analyser based on the information of a given operator.
   *
   * @param operator The operator's data.
   */
  public Analyser(OperatorInfo operator) {
    this.operator = operator;
    sheddingRates = new Metrics(operator.name, "shares", operator.outputTypes.length * operator.inputTypes.length + 1);
    lambdaIn = new Metrics(operator.name, "lambdaIn", operator.inputTypes.length + 1);
    ptime = new Metrics(operator.name, "ptime", operator.outputTypes.length);
    for (String ptimeKey : operator.outputTypes) {
      ptime.put(ptimeKey, 0.);
    }
    for (String lambdaKey : operator.inputTypes) {
      lambdaIn.put(lambdaKey, 0.);
      for (String ptimeKey : operator.outputTypes) {
        sheddingRates.put(ptimeKey + "_" + lambdaKey, 0.);
      }
    }
    ptime.put("total", 0.);
    lambdaIn.put("total", 0.);
    lastAverage = operator.latencyBound;
    lastPtime = lastAverage;
    lastLambda = operator.controlBatchSize;
  }

  /**
   * Process the incoming metrics, calculating the average processing time of an operator
   * and determining if this time surpasses a given latency bound.
   * If the operator's rates vary in at least 5% from one measurement to the other, or the
   * average processing rate varies in at least 10% between measurements or if the average
   * processing time or the total time processing time of an operator exceed a given
   * latency bound, then the analyser contacts the coordinator by sending it a job message.
   * (empty Metrics with description: overloaded).
   *
   * @param value The input value.
   * @param ctx   A {@link Context} that allows querying the timestamp of the element and getting a
   *              TimerService for registering timers and querying the time. The context is only
   *              valid during the invocation of this method, do not store it.
   * @param out   The collector for returning result values.
   * @throws Exception Flink's error
   */
  @Override
  public void processElement(Metrics value, ProcessFunction<Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    switch (value.description) {
      case "shares": {
        sheddingRates = value;
        if (!isShedding && value.get("shedding").equals(Double.POSITIVE_INFINITY)) {
          isShedding = true;
        } else if (isShedding && value.get("shedding").equals(Double.NEGATIVE_INFINITY)) {
          isShedding = false;
        }
        isCoordinatorInformed = false;
        return;
      }
      case "lambdaIn": {
        lambdaIn = value;
        long currentTime = System.currentTimeMillis();
        if (arrivalTimePtime == -1) {
          arrivalTimePtime = currentTime;
        }
        deltaX = currentTime - arrivalTimePtime;
        arrivalTimeLambda = currentTime;
        break;
      }
      case "ptime": {
        ptime = value;
        long currentTime = System.currentTimeMillis();
        if (arrivalTimeLambda == -1) {
          arrivalTimeLambda = currentTime;
        }
        deltaX = currentTime - arrivalTimeLambda;
        arrivalTimePtime = currentTime;
      }
    }

    double totalLambda = lambdaIn.get("total");
    double totalPtime = ptime.get("total");
    double calculatedP = 0.;
    for (String key : operator.inputTypes) {
      double weight = 0;
      for (String key2 : operator.outputTypes) {
        double share = isShedding ? (1 - sheddingRates.get(key2 + "_" + key)) : 1;
        weight += share * ptime.get(key2);
      }
      calculatedP += (lambdaIn.get(key) / (totalLambda == 0 ? 1 : totalLambda)) * weight;
    }


    double B =
      Math.max(0,(1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - totalLambda)));
    boolean pHasChanged = calculatedP > 1.1 * lastAverage;
    boolean lambdaHasChanged = totalLambda > 1.05 * lastLambda;
    boolean ptimeHasChanged = totalPtime > 1.05 * lastPtime;

    double upperBound = 1 / ((1 / operator.latencyBound) + totalLambda);
    double lowerBound = upperBound * 0.9;
    double bound = operator.latencyBound;
    if (!isCoordinatorInformed && calculatedP > lowerBound || ((pHasChanged || lambdaHasChanged || ptimeHasChanged) && B > bound) || (deltaX/1000.) > 1) {
      informCoordinator(value.name, out);
      isCoordinatorInformed = true;
    } else if(isShedding && ((0 < B && B < bound) || totalPtime < lowerBound)) {
      isShedding = false;
      ctx.output(toKafka,operator.name);
    }
    lastAverage = calculatedP;
    lastPtime = ptime.get("total");
    lastLambda = totalLambda;

  }

  /**
   * For global load shedding.
   * Informs the coordinator that the operator corresponding to this analyser is overloaded.
   *
   * @param operatorsName Name of the operator.
   * @param out           Collector of this operator's outputs.
   */
  private void informCoordinator(String operatorsName, Collector<Metrics> out) {
    Metrics empty = new Metrics(operatorsName, "overloaded", 0);
    out.collect(empty);
  }

}
