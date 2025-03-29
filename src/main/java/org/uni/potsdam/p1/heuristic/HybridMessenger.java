package org.uni.potsdam.p1.heuristic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

public class HybridMessenger extends ProcessFunction<String, String>{

    OperatorInfo operator;
    public String opName;
    StringOutput kafkaToAnalyser;
    StringOutput kafkaToOperator;
    Pattern headerPattern;

    private static final Logger meLog = LoggerFactory.getLogger(HybridMessenger.class);

    public HybridMessenger(OperatorInfo operator, StringOutput k2A, StringOutput k2O) {
        this.operator = operator;
        opName = operator.name;
        kafkaToAnalyser = k2A;
        kafkaToOperator = k2O;
        headerPattern = Pattern.compile("target:([^\\s]+) description:([^\\s]+) origin:([^\\s]+) pattern:([^\\s]+) timestamp:([^\\s]+)");
    }

    @Override
    public void processElement(String msg, ProcessFunction<String, String>.Context ctx, Collector<String> out)
            throws RuntimeException {
        String[] segments = msg.split(Pattern.quote("~"));
        String header = segments[0];
        
        //parse the header to check if we are processing the correct message 
        Matcher matchHeader = headerPattern.matcher(header);
        
        String msgTarget = "";
        String desc = "";
        String origin = "";
        
        // decode header
        if (matchHeader.find()) {
            msgTarget = matchHeader.group(1);
            desc = matchHeader.group(2);
            origin = matchHeader.group(3);
            String pattern = matchHeader.group(4);
            String timestamp = matchHeader.group(5);

            
            if (desc.equals("") || origin.equals("") || segments.length < 2) {
                throw new RuntimeException("Message parsing error in the hybrid messenger component or message creation error in the hybrid analyser");
            }
            
            if (!desc.equals("aggSelectivity") || origin.equals(opName)) {
                meLog.info("Forwarding kafka message from " + opName + " messenger to operator in " + msgTarget + " for " + msg);
                ctx.output(kafkaToOperator, msg);
            } else {
                meLog.info("Forwarding kafka message from " + opName + " messenger to analyzer in " + msgTarget + " for " + msg);
                ctx.output(kafkaToAnalyser, msg);
            }
        } else {
            meLog.error("Error forwarding message in HybridMessenger " + opName);
            throw new RuntimeException("Message parsing error in the hybrid messenger component or message creation error in the hybrid analyser");
        }
    }
}
