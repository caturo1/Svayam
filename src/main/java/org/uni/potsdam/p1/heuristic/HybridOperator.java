package org.uni.potsdam.p1.heuristic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.apache.flink.util.Collector;

/**
 * The actual implementation of the Operator's logic including its
 * overload detection, load shedding calculation,
 */

public class HybridOperator extends CoProcessFunction<Event, String, Event> {
    
    public final HybridOperatorCore core;
    public Metrics sheddingRates;

    public Pattern headerPattern;
    public Pattern mapPattern;

    public boolean isShedding;
    
    public HybridOperator(OperatorInfo operatorInfo) {
        this.core = new HybridOperatorCore(operatorInfo);
        sheddingRates = core.sheddingRates;
        isShedding = core.isShedding;

        headerPattern = Pattern.compile("target:([^\\s]+) description:([^\\s]+) origin:([^\\s]+) pattern:([^\\s]+) timestamp:([0-9]+)");
        mapPattern = Pattern.compile("\\|?\\s*([^\\s|=]+)\\s*=\\s*([0-9]*\\.?[0-9]+)");
    }

    public void setSideOutput(String operatorName, EventOutput operatorOut) {
        core.setSideOutput(operatorName, operatorOut);
    }

    public void setMetricsOutput(String metric, MetricsOutput metricsOut) {
        core.setMetricsOutput(metric, metricsOut); 
    }
        
    @Override
    public void processElement1(Event event, CoProcessFunction<Event, String, Event>.Context ctx, Collector<Event> out)
    throws Exception {
            core.processWithContext(event, ctx);
    }

    public void integrateRates(String mapToken) {
        Matcher selMatcher = mapPattern.matcher(mapToken);

        while (selMatcher.find()) {
            String key = selMatcher.group(1);
            Double value = Double.valueOf(selMatcher.group(2));
            sheddingRates.put(key, value);
        } 
    }

    @Override
    public void processElement2(String msg, CoProcessFunction<Event, String, Event>.Context ctx,
            Collector<Event> out) throws Exception {
        
        String[] segments = msg.split(Pattern.quote("~"));
        String header = segments[0];
        String mapToken = segments[1];
    
        //parse the header to check, if 
        
        Matcher headerMatcher = headerPattern.matcher(header);
        
        String desc = "";
        // not sure if I shall implement a shedding delay (Sukanya said sth about this but it doesn't make any sense 2me currently)
        long timestamp = 0;
        
        if (headerMatcher.find()) {
            desc = headerMatcher.group(2);
            timestamp = Long.parseLong(headerMatcher.group(5));
        }

        switch(desc) {
            case "startShedding" : 
                isShedding = true;
                break;
            
            case "stopShedding" :
                isShedding = false;
                break;

            // covers the case of sending the initial map and updated selectivities for just the inputs to one specific pattern
            // because it doesn't make a difference from a parsing perspective and we update the object the core uses
            case "sheddingRatesMap" : 
                integrateRates(mapToken);
                break;

            default:
                throw new IllegalStateException("Error while processing a message from the analyzer in the operator");
        }
        
    }

}
