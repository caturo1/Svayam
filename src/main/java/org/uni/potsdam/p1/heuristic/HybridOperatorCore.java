package org.uni.potsdam.p1.heuristic;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.operators.cores.OperatorCore;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;


/**
 * Specification of a HybridOperator's components and setup and processing logic
 * It also forwards its output rates in case of a request from upstream
 */
public class HybridOperatorCore extends OperatorCore {

    CoProcessFunction<Event, String, Event>.Context ctx;
    // I think this should be a StringOutput now
    public Set<MetricsOutput> AnalyserInputs;

    public HybridOperatorCore(OperatorInfo operator) {
        super(operator);
        AnalyserInputs = new HashSet<>();
    }

    @Override
    protected void processSideOutputs(EventPattern pattern, Event value) {
        for (String downstreamOp : pattern.downstreamOperators) 
            ctx.output(extraOutputs.get(downstreamOp), value);
        }

    @Override
    protected void processMeasuredRates() {
        // Update and emit metrics when measurers are ready
        if (processingTimesMeasurer.isReady()) {
            updateAndForward(processingTimesMeasurer, processingTimes, ctx);
            updateAndForward(processingRateMeasurer, processingRates, ctx);      
            // Log stats
            opLog.info(String.format("{\"ptime\":%f,\"time\":%d,\"name\":\"%s\"}", processingTimesMeasurer.results.get("total"), System.currentTimeMillis(), operator.name));
        }

        if (outputRateMeasurer.isReady()) {
            // Forward output rates
            updateAndForward(outputRateMeasurer, AnalyserInputs, ctx);
        }
    }

    public void updateAndForward(Measurer<?> measurer, Set<MetricsOutput> metricsOutput, CoProcessFunction<Event, String, Event>.Context ctx) {
        Metrics currentMetrics = measurer.getNewestAverages();
        for(MetricsOutput out : metricsOutput) {
          ctx.output(out,currentMetrics);
        }
    }

    public void updateAndForward(Measurer<?> measurer, MetricsOutput metricsOutput, CoProcessFunction<Event, String, Event>.Context ctx) {
        Metrics currentMetrics = measurer.getNewestAverages();
        if (metricsOutput != null) {
          ctx.output(metricsOutput, currentMetrics);
        }
    }

    /**
     * Process a measurement with context
     */
    public void processWithContext(Event value, CoProcessFunction<Event, String, Event>.Context ctx) {
        this.ctx = ctx;
        super.process(value);
    }
    
    @Override
    public void setMetricsOutput(String metric, MetricsOutput whereTo) {
      switch (metric) {
        case "lambdaOut": {
          AnalyserInputs.add(whereTo);
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