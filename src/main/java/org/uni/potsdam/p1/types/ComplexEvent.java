package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

public class ComplexEvent implements Serializable {
  public final String name;
  public final long timestamp;

  public ComplexEvent() {
    name = "";
    timestamp = System.nanoTime();
  }

  public ComplexEvent(String name, long timestamp) {
    this.name = name;
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ComplexEvent that = (ComplexEvent) o;
    return timestamp == that.timestamp && Objects.equals(name, that.name);
  }

  @Override
  public String toString() {
    return "ComplexEvent{" +
      "name='" + name + '\'' +
      ", timestamp=" + timestamp +
      '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, timestamp);
  }
}
