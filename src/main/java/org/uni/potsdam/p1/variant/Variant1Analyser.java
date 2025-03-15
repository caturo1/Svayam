package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

import java.util.Map;

public class Variant1Analyser
  extends CoProcessFunction<Metrics, String, Metrics> {
  // define latest metrics received
  Metrics ptime;
  Metrics lambdaIn;
  Metrics lambdaOut;

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
  public boolean useLocalSheddingRates = false;

  public StringOutput toKafka;

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
  public void processElement1(Metrics value, CoProcessFunction<Metrics, String, Metrics>.Context ctx, Collector<Metrics> out) {
    switch (value.description) {
      case "shares": {
        sheddingRates = value;
        isCoordinatorInformed = false;
        if (!useLocalSheddingRates) {
          isShedding = true;
        }
        return;
      }
      case "lambdaOut": {
        if (lambdaOut != null && value.name.equals(operator.name)) {
          this.lambdaOut = value;
          break;
        }
      }
      case "lambdaIn": {
        double diff = 0;
        for (String eventType : value.map.keySet()) {
          if (!eventType.equals("total") && lambdaIn.map.containsKey(eventType)) {
            double oldValue = lambdaIn.get(eventType);
            double newValue = value.get(eventType);
            diff += (newValue - oldValue);
            lambdaIn.put(eventType, newValue);
          }
        }
        lambdaIn.put("total", lambdaIn.get("total") + diff);
        break;
      }
      case "ptime": {
        ptime = value;
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
      Math.max(0, (1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - totalLambda)));
    boolean pHasChanged = calculatedP > 1.1 * lastAverage;
    boolean lambdaHasChanged = totalLambda > 1.05 * lastLambda;
    boolean ptimeHasChanged = totalPtime > 1.05 * lastPtime;

    double upperBound = 1 / ((1 / operator.latencyBound) + totalLambda);
    double lowerBound = upperBound * 0.9;
    double bound = operator.latencyBound;

    if (!isCoordinatorInformed && (calculatedP >= lowerBound || ((pHasChanged || lambdaHasChanged || ptimeHasChanged) && B > bound))) {
      if (!isShedding && useLocalSheddingRates) {
        ctx.output(toKafka, calculateSheddingRate());
        isShedding = true;
      }
      informCoordinator(value.name, out);
      isCoordinatorInformed = true;
    } else if (isShedding && (/*(0 < B && B < bound) ||*/ totalPtime < lowerBound)) {
      isShedding = false;
      ctx.output(toKafka, operator.name);
    }
    lastAverage = calculatedP;
    lastPtime = totalPtime;
    lastLambda = totalLambda;
  }

  @Override
  public void processElement2(String value, CoProcessFunction<Metrics, String, Metrics>.Context ctx, Collector<Metrics> out) {
    if (value.equals("snap")) {
      out.collect(lambdaIn);
    } else {
      throw new IllegalStateException("False message received. Should be snap.");
    }
  }

  public String calculateSheddingRate() {
    Metrics mus = lambdaIn;
    Metrics lambdaOuts = lambdaOut;
    StringBuilder output = new StringBuilder();
    output.append(operator.name).append(":");
    for (EventPattern eventPattern : operator.patterns) {
      Map<String, Integer> weights = eventPattern.getWeightMaps();
      double sum = 0.;
      for (String types : weights.keySet()) {
        sum += mus.get(types);
      }
      String patternKey = eventPattern.name;
      double total = lambdaOuts.get("total");
      for (String inputType : weights.keySet()) {
        double value;
        if (sum == 0 || total == 0) {
          value = 0;
        } else {
          value = Math.min(1,
            (mus.get(inputType) / (weights.get(inputType) * sum)) * (lambdaOuts.get(patternKey) / total));//*(ptime.get(patternKey) / ptime.get("total")));
        }
        output.append(String.format("%s_%s|%f:", eventPattern.name, inputType, value));
        sheddingRates.put(eventPattern.name + "_" + inputType, value);
      }
    }
    return output.append("A").toString();
  }

  public void withLambdaOut() {
    lambdaOut = new Metrics(operator.name, "lambdaOut", operator.outputTypes.length + 1);
    for (String type : operator.outputTypes) {
      lambdaOut.put(type, 0.);
    }
    lambdaOut.put("total", 0.);
    this.useLocalSheddingRates = true;
  }
}