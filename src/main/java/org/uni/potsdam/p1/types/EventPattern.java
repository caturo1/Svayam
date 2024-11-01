package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

public class EventPattern implements Serializable {
  public String name;
  public String type;
  public String[] downstreamOperators;
  boolean wasVisited;

  public EventPattern() {
  }

  public EventPattern(String name, String type, String[] downstreamOperators) {
    this.name = name;
    this.type = type;
    this.downstreamOperators = downstreamOperators;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventPattern pattern = (EventPattern) o;
    return Objects.equals(name, pattern.name) && Objects.equals(type, pattern.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  public void visit() {
    wasVisited = true;
  }

  public void clear() {
    wasVisited = false;
  }
}
