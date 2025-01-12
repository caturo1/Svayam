package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

public class Variant1Analyser extends CoProcessFunction<Metrics, String,Metrics> {
  // define latest metrics received
  Metrics ptime;
  long arrivalTimePtime = -1;

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
  MetricsOutput sosOutput;
  MetricsOutput inputRates;

  public Variant1Analyser(OperatorInfo operatorInfo) {
    this.operator = operatorInfo;
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

  private void informCoordinator(String operatorsName, Collector<Metrics> out) {
    Metrics empty = new Metrics(operatorsName, "overloaded", 0);
    out.collect(empty);
  }

//  long timeout;

  @Override
  public void processElement1(Metrics value, CoProcessFunction<Metrics, String, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    switch (value.description) {
      case "shares": {
        sheddingRates = value;
        isCoordinatorInformed = false;
        isShedding = true;

        //TODO test different timeouts
//        timeout = System.currentTimeMillis()+(long)(lastLambda*lastPtime*1000);
//        timeout = System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(5);
        return;
      }
      case "lambdaOut": {
        double diff = 0;
        for(String eventType :  value.map.keySet()) {
          if(!eventType.equals("total") && lambdaIn.map.containsKey(eventType)) {
            double oldValue = lambdaIn.get(eventType);
            double newValue = value.get(eventType);
            diff += (newValue-oldValue);
            lambdaIn.put(eventType,newValue);
          }
        }
        lambdaIn.put("total",lambdaIn.get("total")+diff);
        break;
      }
      case "ptime": {
        ptime = value;
        long currentTime = System.currentTimeMillis();
        if (arrivalTimeLambda == -1) {
          arrivalTimeLambda = currentTime;
        }
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

//    boolean isOverloaded = (pHasChanged || lambdaHasChanged || ptimeHasChanged) && B > bound;
//    boolean isUnderloaded = isShedding && 0 < B && B < bound;


    if (!isCoordinatorInformed && (calculatedP > lowerBound || ((pHasChanged || lambdaHasChanged || ptimeHasChanged) && B > bound))) {
      informCoordinator(value.name, out);
      isCoordinatorInformed = true;
    } else if(isShedding && ((0 < B && B < bound) || totalPtime < lowerBound)) {
      isShedding = false;
      ctx.output(toKafka,operator.name);
    }
    // TODO test different activation logic - use ptime for activating shedding and p for demanding a new shedding config
//    if (!isCoordinatorInformed &&
//      (calculatedP > lowerBound || isOverloaded || isUnderloaded)
//      && System.currentTimeMillis() > timeout) {
//      informCoordinator(value.name, out);
//      isCoordinatorInformed = true;
//    } else if(isShedding && (totalPtime < lowerBound)) {
//      isShedding = false;
//      ctx.output(toKafka,operator.name);
//      timeout = System.currentTimeMillis()+(long)(lastLambda*lastPtime*1000);
//      timeout = System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(5);
//    }
//    if(System.currentTimeMillis() > timeout) {
//      if (!isCoordinatorInformed &&
//        (calculatedP > lowerBound || isOverloaded || isUnderloaded) ) {
////      && System.currentTimeMillis() > timeout) {
//        informCoordinator(value.name, out);
//        isCoordinatorInformed = true;
//      } else if(isShedding && (totalPtime < lowerBound)) {
//        isShedding = false;
//        ctx.output(toKafka,operator.name);
//      timeout = System.currentTimeMillis()+(long)(lastLambda*lastPtime*1000);
//        timeout = System.currentTimeMillis()+TimeUnit.SECONDS.toMillis(5);
//      }
//    }
    lastAverage = calculatedP;
    lastPtime = totalPtime;
    lastLambda = totalLambda;
  }

  @Override
  public void processElement2(String value, CoProcessFunction<Metrics, String, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if (value.equals("snap")) {
      out.collect(lambdaIn);
    } else {
      throw new IllegalStateException("False message received. Should be snap.");
    }
  }
}
