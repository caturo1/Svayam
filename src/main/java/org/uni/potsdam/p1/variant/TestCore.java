package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.operators.cores.OperatorCore;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.util.*;

public class TestCore
  extends OperatorCore {

  CoProcessFunction<Event, String, Event>.Context ctx;
  public Set<MetricsOutput> downstreamAnalysers;
  public boolean wasVisited = false;

  public TestCore(OperatorInfo operatorInfo) {
    super(operatorInfo);
    downstreamAnalysers = new HashSet<>(operatorInfo.patterns.length);
  }

  @Override
  protected void processSideOutputs(EventPattern pattern, Event value) {
    for (String downstreamOp : pattern.downstreamOperators) {
      ctx.output(extraOutputs.get(downstreamOp), value);
    }
  }

  @Override
  protected void processMeasuredRates() {
    if (processingTimesMeasurer.isReady()) {
      updateAndForward(processingTimesMeasurer, processingTimes, ctx);
      updateAndForward(processingRateMeasurer, processingRates, ctx);
      opLog.info(String.format("{\"ptime\":%f,\"time\":%d,\"name\":\"%s\"}", processingTimesMeasurer.results.get("total"), System.currentTimeMillis(), operator.name));
      double upperBound = 1 / ((1 / operator.latencyBound) + processingRateMeasurer.getTotalAverageRate());
      double lowerBound = upperBound * 0.9;
      factor = processingTimesMeasurer.results.get("total") / lowerBound;
    }

    if (outputRateMeasurer.isReady()) {
      updateAndForward(outputRateMeasurer, downstreamAnalysers, ctx);
    }
  }

  public void updateAndForward(Measurer<?> measurer, Set<MetricsOutput> metricsOutput, CoProcessFunction<Event, String, Event>.Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    for (MetricsOutput out : metricsOutput) {
      ctx.output(out, currentMetrics);
    }
  }

  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, CoProcessFunction<Event, String, Event>.Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    if (metricsOutput != null) {
      ctx.output(metricsOutput, currentMetrics);
    }
  }

  public void processWithContext(Event value, CoProcessFunction<Event, String, Event>.Context ctx) {
    this.ctx = ctx;
    super.process(value);
  }

  public void processMessages(String value, CoProcessFunction<Event, String, Event>.Context ctx) {

    if (value.equals(operator.name)) {
      if (isShedding) {
        isShedding = false;
        opLog.info(operator.getSheddingInfo(isShedding, sheddingRates));
      }
      return;
    }
    if (value.equals("snap")) {
      ctx.output(sosOutput, processingRateMeasurer.getMetrics());
      ctx.output(sosOutput, processingTimesMeasurer.getMetrics());

    } else {
      int index = value.indexOf(":");
      int len = value.length();
      boolean forward = true;
      boolean comesFromAnalyser = value.endsWith("A");
      if (comesFromAnalyser) {
        wasVisited = true;
        forward = false;
        len--;
        isShedding = true;
      }
      for (String share : value.substring(index + 1, len).split(":")) {
        int separationIndex = share.indexOf("|");
        String currentShare = share.substring(separationIndex + 1);
        double shareNumber;
        try {
          shareNumber = Double.parseDouble(currentShare);
        } catch (NumberFormatException e) {
          System.err.println("Problem with " + currentShare);
          System.err.println(value);
          throw e;
        }
        sheddingRates.put(share.substring(0, separationIndex), Math.min(0.9,factor*shareNumber));
      }
      if (!isShedding && !wasVisited) {
        isShedding = true;
      }
      opLog.info(operator.getSheddingInfo(isShedding, sheddingRates) + (comesFromAnalyser ? "A" : ""));
      if (forward) {
        ctx.output(processingTimes, sheddingRates);
      }
    }
  }

  @Override
  public void setMetricsOutput(String metric, MetricsOutput whereTo) {
    switch (metric) {
      case "lambdaOut": {
        downstreamAnalysers.add(whereTo);
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

}
