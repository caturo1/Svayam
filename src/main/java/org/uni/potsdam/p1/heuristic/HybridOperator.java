package org.uni.potsdam.p1.heuristic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    
    private static final Logger oLog = LoggerFactory.getLogger(HybridOperator.class);
    public final HybridOperatorCore core;
    public Pattern headerPattern;
    public Pattern mapPattern;
    
    public HybridOperator(OperatorInfo operatorInfo) {
        this.core = new HybridOperatorCore(operatorInfo);
        // this might never reach the actual core

        headerPattern = Pattern.compile("target:([^\\s]+) description:([^\\s]+) origin:([^\\s]+) pattern:([^\\s]+) timestamp:([^\\s]+) messageID:([^\\s]+)");
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
            core.sheddingRates.put(key, value);
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
        
        String msgTarget = "default";
        String desc = "default";
        String origin = "default";
        String pattern = "default";
        // not sure if I shall implement a shedding delay (Sukanya said sth about this but it doesn't make any sense 2me currently)
        
        if (headerMatcher.find()) {
            msgTarget = headerMatcher.group(1);
            desc = headerMatcher.group(2);
            origin = headerMatcher.group(3);
            pattern = headerMatcher.group(4);
            String ts = headerMatcher.group(5);
            String id = headerMatcher.group(6);
        }

        //oLog.info("Received kafka message in " + core.operator.name + " with description: " + desc + ". Set for shedding coordination.");

        switch(desc) {
            case "startShedding" : 
                core.isShedding = true;
                break;
            
            case "stopShedding" :
                core.isShedding = false;
                break;

            // covers the case of sending the initial map and updated selectivities for just the inputs to one specific pattern
            // because it doesn't make a difference from a parsing perspective and we update the object the core uses
            case "sheddingRatesMap" : 
                integrateRates(mapToken);
                //oLog.info("Updated sheddingRates in operator " + core.operator.name + "for message: " + msg);
                break;
            
            case "aggSelectivity" :
                throw new IllegalArgumentException("The operator " + core.operator.name + "should not receive aggregate selectivities");

            default:
                throw new IllegalStateException("Error while processing a message from the analyzer in the operator");
        }
        
    }

}
