package org.uni.potsdam.p1.types;

import org.uni.potsdam.p1.actors.measurers.Measurer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This classed is designed to store the information of a specific metric of an operator.
 * see {@link Measurer} and {@link OperatorInfo} for more information.
 */
public class Metrics implements Serializable {
  public String name;
  public String description;
  public Map<String, Double> map;

  /**
   * @param name        Name of the corresponding operator
   * @param description Metric type see: {@link OperatorInfo}
   * @param capacity    Number of event types to be considered
   */
  public Metrics(String name, String description, int capacity) {
    this.name = name;
    this.description = description;
    map = new HashMap<>(capacity);
  }

  public Metrics() {
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public String toString() {
//    return "{ \"type\": " + type + ", \"time\": " + eventTime + "}";
    return name + " " + description + " " + map.toString();
//    return map.toString();
  }

  public void put(String key, Double value) {
    map.put(key, value);
  }

  public Double get(String key) {
    return map.get(key);
  }

  public Set<Map.Entry<String, Double>> entrySet() {
    return map.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Metrics metrics = (Metrics) o;
    return name.equals(metrics.name) && description.equals(metrics.description) && Objects.equals(map, metrics.map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, map);
  }

  /**
   * Calculates the smallest value stored in this Metrics instance.
   *
   * @return Minimum value stored in this object.
   * @throws IllegalStateException if this objects is empty.
   */
  public double getMinimum() {
    double min = Double.POSITIVE_INFINITY;
    for (double value : map.values()) {
      if (value < min) {
        min = value;
      }
    }
    if (min == Double.POSITIVE_INFINITY) {
      throw new IllegalStateException("Pattern is not initialised.");
    }
    return min;
  }

  /**
   * Calculates the smallest value stored in this Metrics instance weighting each instance
   * in accordance to a given map. This map must contain key values present in this
   * {@link Metrics} object or else the method will fail.
   *
   * @return Minimum value stored in this object for all shared keys with the map.
   * @throws IllegalStateException if this objects is empty.
   */
  public double getWeightedMinimum(Map<String, Integer> weights) {
    double min = Double.POSITIVE_INFINITY;
    for (Map.Entry<String, Integer> weight : weights.entrySet()) {
      double value = map.get(weight.getKey()) / weight.getValue();
      if (value < min) {
        min = value;
      }
    }
    if (min == Double.POSITIVE_INFINITY) {
      throw new IllegalStateException("Pattern is not initialised.");
    }
    return min;
  }

  /**
   * Updates all values of this map with zero.
   *
   * @return Zeroed Metrics object.
   */
  public Metrics withZeroedValues() {
    map.replaceAll((k, v) -> 0.);
    return this;
  }

  /**
   * Calculates the highest value stored in this Metrics instance weighting each instance
   * in accordance to a given map. This map must contain key values present in this
   * {@link Metrics} object or else the method will fail.
   *
   * @return Maximum value stored in this object for all shared keys with the map.
   * @throws IllegalStateException if this objects is empty.
   */
  public double getWeightedMaximum(Map<String, Integer> weights) {
    double max = Double.NEGATIVE_INFINITY;
    for (Map.Entry<String, Integer> weight : weights.entrySet()) {
      double value = map.get(weight.getKey()) / weight.getValue();
      if (value > max) {
        max = value;
      }
    }
    if (max == Double.NEGATIVE_INFINITY) {
      throw new IllegalStateException("Pattern is not initialised.");
    }
    return max;
  }

  /**
   * Calculates the sum of all values stored in this Metrics instance weighting each instance
   * in accordance to a given map. This map must contain key values present in this
   * {@link Metrics} object or else the method will fail.
   *
   * @return Sum of all values stored in this object for all shared keys with the map.
   * @throws IllegalStateException if this objects is empty.
   */
  public double getWeightedSum(Map<String, Integer> weights) {
    double sum = 0.;
    for (Map.Entry<String, Integer> weight : weights.entrySet()) {
      double value = map.get(weight.getKey()) / weight.getValue();
      sum += value;
    }
    if (sum == 0.) {
      throw new IllegalStateException("Pattern is not initialised.");
    }
    return sum;
  }
}
