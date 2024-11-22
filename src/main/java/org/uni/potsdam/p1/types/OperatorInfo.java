package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

/**
 * <p>This class stores the basic information about an operator:</p>
 * <ul>
 *   <li>Name</li>
 *   <li>Input types</li>
 *   <li>Patterns implemented</li>
 *   <li>Metrics:
 *   <ul>
 *     <li>input rates = lambdaIn</li>
 *     <li>output rates = lambdaOut</li>
 *     <li>processing rates = mu</li>
 *     <li>processing time pro pattern = ptime</li>
 *   </ul>
 *   <li>If the operator is a sink or is currently overloaded</li>
 * </ul>
 */
public class OperatorInfo implements Serializable {
  public String[] inputTypes;
  public String[] outputTypes;
  public EventPattern[] patterns;
  public boolean isOverloaded = false;
  public boolean isSinkOperator = false;
  public String name;
  public HashMap<String, Integer> indexer = new HashMap<>(4);
  public Metrics[] metrics = new Metrics[4];
  public int controlBatchSize;
  public double latencyBound;

  /**
   * Constructs an empty {@link OperatorInfo} instance
   */
  public OperatorInfo() {
    String[] metrics = new String[]{"lambdaIn", "lambdaOut", "mu", "ptime"};
    for (int i = 0; i < metrics.length; i++) {
      indexer.put(metrics[i], i);
    }
  }

  public String[] getOutputTypes() {
    return outputTypes;
  }

  /**
   * Constructs an OperatorInfo instance with the desired parameters.
   */
  public OperatorInfo(String name, String[] inputTypes, int controlBatchSize, double latencyBound, EventPattern[] patterns, boolean isSinkOperator) {
    this();
    this.inputTypes = inputTypes;
    this.patterns = patterns;
    this.isSinkOperator = isSinkOperator;
    this.name = name;
    this.controlBatchSize = controlBatchSize;
    this.latencyBound = latencyBound;
    this.outputTypes = Arrays.stream(patterns).map(EventPattern::getName).toArray(String[]::new);
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

  public Metrics getMetric(String metric) {
    return metrics[indexer.get(metric)];
  }

  public Double getValue(String metric, String value) {
    return metrics[indexer.get(metric)].get(value);
  }

  public String[] getInputTypes() {
    return inputTypes;
  }

  public void setInputTypes(String[] inputTypes) {
    this.inputTypes = inputTypes;
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

  /**
   * Proofs if an {@link OperatorInfo} object has gathered metrics from every operator.
   *
   * @return true iff all {@link Metrics} objects contained in this object are not null.
   */
  public boolean isReady() {
    for (Metrics metric : metrics) {
      if (metric == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Clears the metrics information contained in this object.
   */
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

  /**
   * Sets the name of an OperatorInfo object to the specified value.
   *
   * @param name the new name
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Sets the size of the control batch size for this operator's {@link org.uni.potsdam.p1.actors.measurers.Measurer}
   * instances.
   *
   * @param size Amount of events needed to calculate the running average of this operator
   *             metric measurers
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo withControlBatchSize(int size) {
    this.controlBatchSize = size;
    return this;
  }

  /**
   * Sets the latency bound for processing events in this operator
   *
   * @param latencyBound Maximum amount of seconds that a process should take to be
   *                     processed in this operator.
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo withLatencyBound(double latencyBound) {
    this.latencyBound = latencyBound;
    return this;
  }

  /**
   * Sets the input types of an OperatorInfo object to the specified value.
   *
   * @param types the event types represented as {@link String}
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo withInputTypes(String... types) {
    this.inputTypes = types;
    return this;
  }

  /**
   * Sets the patterns of an OperatorInfo object to the specified value.
   *
   * @param patterns the patterns represent as one or more {@link EventPattern}
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo withPatterns(EventPattern... patterns) {
    this.patterns = patterns;
    this.outputTypes = Arrays.stream(patterns).map(EventPattern::getName).toArray(String[]::new);
    return this;
  }

  /**
   * Specify if this operator is connected to a sink.
   *
   * @return Reference to the given OperatorInfo-object
   */
  public OperatorInfo toSink() {
    this.isSinkOperator = true;
    return this;
  }

  public String getSheddingInfo(boolean isShedding) {
    return "{ \"isShedding\": " + isShedding + ", \"time\": " + System.currentTimeMillis() + ", \"name\": \"" + name + "\"}";
  }

}
