package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

public class Measurement implements Serializable {
  public final int machineId;
  public final long realMachineId;
  public final long eventTime;
  public int source;
  public String message;

  public Measurement() {
    realMachineId = System.nanoTime();
    machineId = (int) (Math.random() * 4);
    eventTime = System.currentTimeMillis();
  }

  public Measurement(int id, int source, long index) {
    realMachineId = index;
    machineId = id;
    eventTime = System.currentTimeMillis();
    this.source = source;
  }

  public Measurement(int id) {
    realMachineId = System.nanoTime();
    machineId = id;
    eventTime = System.currentTimeMillis();
  }

  public Measurement(int id, String message, int tab) {
    this(id);
    this.message = "\t".repeat(tab) + message;
  }

  public Measurement(long id, long time) {
    realMachineId = System.nanoTime();
    machineId = (int) (realMachineId % 4);
    eventTime = time;
  }

  public Measurement(long plus) {
    realMachineId = System.nanoTime();
    machineId = (int) (realMachineId % 4);
    eventTime = System.currentTimeMillis();
  }

  public Measurement(Measurement toCopy) {
    realMachineId = toCopy.realMachineId;
    machineId = toCopy.machineId;
    eventTime = toCopy.eventTime;/*System.currentTimeMillis();*/
    source = toCopy.source;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Measurement that = (Measurement) o;
    return machineId == that.machineId && eventTime == that.eventTime;/*; && source == that.source;*/
  }

  @Override
  public String toString() {
    return "Measurement{" +
      "real machineId=" + realMachineId +
      ", machineId=" + machineId +
      ", eventTime=" + eventTime +
      ", source=" + source +
      "\nmessage=" + message +
      '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(machineId, eventTime/*,source*/);
  }
}
