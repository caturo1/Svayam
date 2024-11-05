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
  public HashMap<String, Double> map;
  public long id;

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

  @Override
  public String toString() {
//    return name + ":" + description + ":" + map.toString();
    return map.toString();
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
}
