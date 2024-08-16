package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

public class Measurement implements Serializable {
  public final int machineId;
  public final long eventTime;

  public Measurement() {
    machineId = (int) (Math.random() * 4);
    eventTime = System.nanoTime();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Measurement that = (Measurement) o;
    return machineId == that.machineId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(machineId);
  }
}
