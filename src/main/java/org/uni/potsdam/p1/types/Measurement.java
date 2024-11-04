package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

public class Measurement implements Serializable {
  public final int type;
  public final long eventTime;
  public int source;
  public String message;

  public Measurement() {
    type = (int) (Math.random() * 4);
    eventTime = System.currentTimeMillis();
  }

  public Measurement(long index) {
    this();
    message = String.valueOf(index);
  }

  public Measurement(int id, String message, int tab) {
    type = id;
    this.message = "\t".repeat(tab) + message;
    eventTime = System.currentTimeMillis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Measurement that = (Measurement) o;
    return type == that.type && eventTime == that.eventTime;/*; && source == that.source;*/
  }

  @Override
  public String toString() {
    return "Measurement{" +
      " type=" + type +
      ", eventTime=" + eventTime +
      ", source=" + source +
      "\nmessage=" + message +
      '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, eventTime);
  }
}
