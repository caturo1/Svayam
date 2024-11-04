package org.uni.potsdam.p1.actors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Metrics;

import java.util.Map;

public class SCAnalyser extends KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics> {

  MapState<String, Double> map1;
  MapState<String, Double> map2;
  String[] lambdaKeys;
  String[] ptimes;
  String groupName;
  public static double LATENCY_BOUND = 15.15E-6;
  double lastLambda = 0.;
  double lastPtime = 0.;
  OutputTag<String> sosOutput;
  boolean isShedding = false;
  Metrics sheddingRates;

  double lastAverage = 0.;

  public SCAnalyser(String groupName, String[] lambdaKeys, String[] ptimes, OutputTag<String> sosOutput) {
    this.lambdaKeys = lambdaKeys;
    this.ptimes = ptimes;
    this.groupName = groupName;
    this.sosOutput = sosOutput;
    sheddingRates = new Metrics(groupName, "shares", ptimes.length * lambdaKeys.length + 1);
    for (String lambdaKey : lambdaKeys) {
      for (String ptimeKey : ptimes) {
        sheddingRates.put(ptimeKey + "_" + lambdaKey, 0.);
      }
    }
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    map1 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map1", String.class, Double.class));
    map2 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map2", String.class, Double.class));
  }

  @Override
  public void processElement1(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if (map2.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map1.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = value.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : ptimes) {
          double share = (1 - sheddingRates.get(key2 + "_" + key));
          weight += share * map2.get(key2);
        }
        calculatedP += (value.get(key) / (total == 0 ? 1 : total)) * weight;
      }
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      double ratioLambda = Math.abs(1 - value.get("total") / (lastLambda == 0 ? 1 : lastLambda));
      double ratioPtime = Math.abs(1 - map2.get("total") / (lastPtime == 0 ? 1 : lastPtime));
      if (lastAverage != 0.) {
        if ((calculatedP > LATENCY_BOUND || (ratio > 0.1 || ratioLambda > 0.05 || ratioPtime > 0.05) && B > LATENCY_BOUND) || (isShedding && B < LATENCY_BOUND)) {
          long id = System.nanoTime();
          Metrics empty = new Metrics(value.name, "overloaded", 0);
          empty.id = id;
          out.collect(empty);
//          ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.getMetric("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
        }
      }
      lastAverage = calculatedP;
      lastPtime = map2.get("total");
      lastLambda = total;
    }
  }

  @Override
  public void processElement2(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if (value.description.equals("shares")) {
      sheddingRates = value;
      if (!isShedding && value.get("shedding").equals(Double.POSITIVE_INFINITY)) {
        isShedding = true;
      } else if (isShedding && value.get("shedding").equals(Double.NEGATIVE_INFINITY)) {
        isShedding = false;
      }
      return;
    }
    if (map1.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map2.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = map1.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : ptimes) {
          double share = (1 - sheddingRates.get(key2 + "_" + key));
          weight += share * value.get(key2);
        }
        calculatedP += (map1.get(key) / (total == 0 ? 1 : total)) * weight;
      }
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      double ratioLambda = Math.abs(1 - map1.get("total") / (lastLambda == 0 ? 1 : lastLambda));
      double ratioPtime = Math.abs(1 - value.get("total") / (lastPtime == 0 ? 1 : lastPtime));
      if (lastAverage != 0.) {
        if ((calculatedP > LATENCY_BOUND || (ratio > 0.1 || ratioLambda > 0.05 || ratioPtime > 0.05) && B > LATENCY_BOUND) || (isShedding && B < LATENCY_BOUND)) {
          long id = System.nanoTime();
          Metrics empty = new Metrics(value.name, "overloaded", 0);
          empty.id = id;
          out.collect(empty);
//          ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.getMetric("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
        }
      }
      lastAverage = calculatedP;
      lastPtime = value.get("total");
      lastLambda = total;
    }
  }
}
