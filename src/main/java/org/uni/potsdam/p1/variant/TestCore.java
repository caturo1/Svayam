package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.operators.cores.OperatorCore;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.util.*;

public class TestCore extends OperatorCore {

  CoProcessFunction<Measurement, String, Measurement>.Context ctx;
  public Set<MetricsOutput> downstreamAnalysers;

  public TestCore(OperatorInfo operatorInfo) {
    super(operatorInfo);
    downstreamAnalysers = new HashSet<>(operatorInfo.patterns.length);
  }

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
      opLog.info(String.format("{\"ptime\":%f,\"time\":%d,\"name\":\"%s\"}", processingTimesMeasurer.results.get("total"), System.currentTimeMillis(), operator.name));
    }

    if (outputRateMeasurer.isReady()) {
      updateAndForward(outputRateMeasurer, downstreamAnalysers, ctx);
    }
  }

  public void updateAndForward(Measurer<?> measurer, Set<MetricsOutput> metricsOutput, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    for(MetricsOutput out : metricsOutput) {
      ctx.output(out,currentMetrics);
    }
  }

  public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {
    Metrics currentMetrics = measurer.getNewestAverages();
    if (metricsOutput != null) {
      ctx.output(metricsOutput, currentMetrics);
    }
  }

  public void processWithContext(Measurement value, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {
    this.ctx = ctx;
    super.process(value);
  }

  public void processMessages(String value, CoProcessFunction<Measurement, String, Measurement>.Context ctx) {

    if(isShedding && value.equals(operator.name )) {
      isShedding = false;
      opLog.info(operator.getSheddingInfo(isShedding));
      return;
    }
    if (value.equals("snap")) {
      ctx.output(sosOutput, processingRateMeasurer.getMetrics());
      ctx.output(sosOutput, processingTimesMeasurer.getMetrics());

    } else {
      int index = value.indexOf(":");
      if (index > 0 && value.substring(0, index).equals(operator.name)) {
        for (String share : value.substring(index + 1).split(":")) {
          int separationIndex = share.indexOf("|");
          String currentShare = share.substring(separationIndex + 1);
          double shareNumber = Double.parseDouble(currentShare);
          sheddingRates.put(share.substring(0, separationIndex),shareNumber);
        }
        if (!isShedding) {
          isShedding = true;
          opLog.info(operator.getSheddingInfo(isShedding));
        }
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
