package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class OperatorInfo implements Serializable {
  public String[] inputTypes;
  public EventPattern[] patterns;
  public boolean isOverloaded = false;
  public boolean isSinkOperator = false;
  public String name;
  public HashMap<String, Integer> indexer = new HashMap<>(4);
  public Metrics[] metrics = new Metrics[4];

  public OperatorInfo() {
  }

  public OperatorInfo(String name, String[] inputTypes, EventPattern[] patterns, boolean isSinkOperator) { //} EventPattern... patterns) {
    this.inputTypes = inputTypes;
    String[] metrics = new String[]{"lambdaIn", "lambdaOut", "mu", "ptime"};
    for (int i = 0; i < metrics.length; i++) {
      indexer.put(metrics[i], i);
    }
    this.patterns = patterns;
    this.isSinkOperator = isSinkOperator;
    this.name = name;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (String index : indexer.keySet()) {
      String toAppend = String.format("%-10s\t%s\n", index + ":", metrics[indexer.get(index)]);
      result.append(toAppend);
    }
    return result.toString();
  }

  public EventPattern getPattern(String patternName) {
    for (EventPattern pattern : patterns) {
      if (pattern.name.equals(patternName)) {
        return pattern;
      }
    }
    throw new IllegalArgumentException("Pattern not contained in the operator.");
  }

  public Metrics get(String key) {
    return metrics[indexer.get(key)];
  }

  public void put(String key, Metrics value) {
    metrics[indexer.get(key)] = value;
  }

  public boolean hasPattern(String pattern) {
    for (EventPattern opPattern : patterns) {
      if (opPattern.name.equals(pattern)) {
        return true;
      }
    }
    return false;
  }

  public boolean isReady() {
    for (Metrics metric : metrics) {
      if (metric == null) {
        return false;
      }
    }
    return true;
  }

  public void clear() {
    Arrays.fill(metrics, null);

    // TODO implement the downstream navigation of the operator graph to make this method relevant
//    for (EventPattern pattern : patterns) {
//      pattern.clear();
//    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OperatorInfo operatorInfo = (OperatorInfo) o;
    return isOverloaded == operatorInfo.isOverloaded && Objects.deepEquals(inputTypes, operatorInfo.inputTypes) && Objects.equals(name, operatorInfo.name) && Objects.equals(indexer, operatorInfo.indexer) && Objects.deepEquals(metrics, operatorInfo.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(inputTypes), isOverloaded, name, indexer, Arrays.hashCode(metrics));
  }
}
