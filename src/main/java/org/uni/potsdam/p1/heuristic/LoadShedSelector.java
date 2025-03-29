package org.uni.potsdam.p1.heuristic;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;

public class LoadShedSelector implements Serializable {
    
    public OperatorInfo operator;
    public Map<String, Double> inputRates;
    public Map<String,Double> outputRates;
    public Map<String, Double> ptimes; 
    public Metrics sheddingRates;
    public Map<String,Double> selectivities; 
    public double bound;
    public double pStar;
    private static final Logger sLog = LoggerFactory.getLogger(LoadShedSelector.class);
    
    public LoadShedSelector(OperatorInfo operator, Map<String,Double> selectivities, Map<String,Double> inputRates, Map<String,Double> outputRates, Map<String,Double> ptimes) {
        this.operator = operator;
        this.selectivities = selectivities;
        this.inputRates = inputRates;
        this.outputRates = outputRates;
        this.ptimes = ptimes;
        bound = operator.latencyBound;
        
        this.sheddingRates = new Metrics(operator.name, "shares", operator.outputTypes.length * operator.inputTypes.length + 1);

        for (String inputType : operator.inputTypes) {
            for (String outType : operator.outputTypes) {
                sheddingRates.put(outType + "_" + inputType, 0.);
            }
        }

        sLog.info("Shedder initiated with pstar" + pStar + "and running data structures {} ", selectivities != null && inputRates!=null && outputRates != null && ptimes != null);
    }

    /**
     * Calculate shedding rates via side effects on the objects
     * such that the analyzer itself can forward the information properly
     */
    public void calculateSheddingRates() {
        // optimal processing time
        pStar = 1. / ((1. / bound) + inputRates.get("total"));
        double totalProcessingTime = ptimes.get("total");
        double totalInputRate = inputRates.get("total");
        double totalOutputRate = outputRates.get("total");
        // current processing tme: sum_{t in T} (lambda_t / lamdba_in) * ptime_t
        double p = Math.max(0, Arrays.stream(operator.inputTypes).map(inputType -> 
            (totalInputRate > 0 ? inputRates.get(inputType) / totalInputRate : 0.) * totalProcessingTime).reduce(0., Double::sum));
        double violation = Math.max(0, pStar > 0 ? (1 - pStar / p) : 0);
        sLog.info("Data structures safe to access: {}", this.selectivities != null && this.inputRates!=null && this.outputRates != null && this.ptimes != null);
        sLog.info("Currenly: p: " + p + "p*: " + pStar + "violation: " + violation);
        try {
            if (violation <= 0) {
                for (String key : sheddingRates.map.keySet()) {
                    sheddingRates.put(key, 0.);
                }
                return;
            }
    
            for (EventPattern pattern : operator.patterns) {
                String patternName = pattern.name;
                Map<String, Integer> weights = operator.getPattern(patternName).getWeightMaps();
            
                double patternPtime = ptimes.getOrDefault(patternName, 0.);
                double patternOutputRate = outputRates.getOrDefault(patternName, 0.);
                double patternSelectivity = selectivities.getOrDefault(patternName, 0.);
                double patternImportance = patternOutputRate > 0 ? patternSelectivity / outputRates.getOrDefault("total", 1.) : 0.;
    
                double patternSpecificInput = 0.;
                for (String inputType : weights.keySet()) {
                    patternSpecificInput += inputRates.getOrDefault(inputType, 0.);
                }
    
                for (String input : weights.keySet()) {
                    int weight = weights.get(input);
                    double inputRate = inputRates.getOrDefault(input, 0.);
                    double inputRatio = patternSpecificInput > 0 ? inputRate / patternSpecificInput : 0;
                    double processingShare = patternPtime > 0 ? patternPtime / totalProcessingTime : 0;
                    double inputImportance = inputRatio * (1. / weight) * processingShare;
                    double combinedImportance = patternImportance * inputImportance;
                    double sheddingRate = Math.max(0, violation * (1. - combinedImportance));
    
                    sheddingRates.put(patternName + "_" + input, sheddingRate);
                }
            }
            sLog.info("Calculated selectivity initial sheddingRates in " + operator.name);
        }

        catch (Exception e) {
            sLog.info("Exception while calculating sheddingRates " + e);
        }
    }
    

    /**
     * Calculate shedding rates for one pattern exclusively.  
     * This involves preprocessing steps and is used upon a significant change in selectivity values.
     *
     * @param patternName name of the pattern, whose sheddingRate we will have to update according to the latest selectivity and data from the analyzer
     */
    public Map<String,Double> calculateSheddingRates(String patternName) {
        pStar = 1. / ((1. / bound) + inputRates.get("total"));
        double totalProcessingTime = ptimes.get("total");
        double totalInputRate = inputRates.get("total");
        double totalOutputRate = outputRates.get("total");
        // current processing tme: sum_{t in T} (lambda_t / lamdba_in) * ptime_t
        double p = Arrays.stream(operator.inputTypes).map(inputType -> (totalInputRate>0.?inputRates.get(inputType) / totalInputRate : 0.) * totalProcessingTime).reduce(0., Double::sum);
        double violation = Math.max(0, pStar > 0 ? (1 - pStar / p) : 0);
        sLog.info("Data structures safe to access: {}", this.selectivities != null && this.inputRates!=null && this.outputRates != null && this.ptimes != null);

        try {
            Map<String, Double> patternSpecificRates = new HashMap<>();
            Map<String, Integer> weights = operator.getPattern(patternName).getWeightMaps();
            
            double patternPtime = ptimes.getOrDefault(patternName, 0.);
            double patternOutputRate = outputRates.getOrDefault(patternName, 0.);
            double patternSelectivity = selectivities.getOrDefault(patternName, 0.);
            double patternImportance = patternOutputRate > 0 ? patternSelectivity / outputRates.getOrDefault("total", 1.) : 0.;
    
            double patternSpecificInput = 0.;
            for (String inputType : weights.keySet()) {
                patternSpecificInput += inputRates.getOrDefault(inputType, 0.);
            }
            
            for (String input : weights.keySet()) {
                int weight = weights.get(input);
                sLog.info("About to calculate rates for pattern: " + patternName + " with weights: " + weights);
                double inputRate = inputRates.getOrDefault(input, 0.);
                double inputRatio = patternSpecificInput > 0 ? inputRate / patternSpecificInput : 0;
                double processingShare = patternPtime > 0 ? patternPtime / totalProcessingTime : 0;
                double inputImportance = inputRatio * (1. / weight) * processingShare;
                double combinedImportance = patternImportance * inputImportance;
                double sheddingRate = Math.max(0, violation * (1. - combinedImportance));
                
                sheddingRates.put(patternName + "_" + input, sheddingRate);
                patternSpecificRates.put(patternName + "_" + input, sheddingRate);
                sLog.info("Calculated selectivity specific sheddingRates in " + operator.name);
            }
            return patternSpecificRates;
        } catch (Exception e) {
            sLog.info("Exception while calculating individual sheddingRates " + e);
            throw new RuntimeException("Exception while calculating individual sheddingRates " + e);
        }
    }
}
