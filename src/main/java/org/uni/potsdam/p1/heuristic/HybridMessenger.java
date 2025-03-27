package org.uni.potsdam.p1.heuristic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

public class HybridMessenger extends ProcessFunction<String, String>{

    OperatorInfo operator;
    public String opName;
    StringOutput kafkaToAnalyser;
    StringOutput kafkaToOperator;
    Pattern headerPattern;


    public HybridMessenger(OperatorInfo operator, StringOutput k2A, StringOutput k2S) {
        this.operator = operator;
        opName = operator.name;
        kafkaToAnalyser = k2A;
        kafkaToOperator = k2S;
        headerPattern = Pattern.compile("target:([^\\s]+) description:([^\\s]+) origin:([^\\s]+)");
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
            
            if (desc.equals("") || origin.equals("") || segments.length < 1) {
                throw new RuntimeException("Message parsing error in the hybrid messenger component or message creation error in the hybrid analyser");
            }
            
            if (!desc.equals("aggSelectivity") || origin.equals(opName)) {
                ctx.output(kafkaToOperator, msg);
            } else {
                ctx.output(kafkaToAnalyser, msg);
            }
        } else {
            throw new RuntimeException("Message parsing error in the hybrid messenger component or message creation error in the hybrid analyser");
        }
    }
}
