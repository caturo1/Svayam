package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class is the main abstraction of the DataStreamJob, representing a CPU-Measurement
 * from the Google-borg-cluster.
 */
public class Measurement implements Serializable {
  public final int type;
  public final long eventTime;
  public String message;

  /**
   * Initialises a new instance of this class with a random event type [0-3] and with a
   * timestamp of the current system clock.
   */
  public Measurement() {
    type = (int) (Math.random() * 4);
    eventTime = System.currentTimeMillis();
  }

  /**
   * Constructor for debugging
   *
   * @param index Specific index value for a measurement (for debugging)
   */
  public Measurement(long index) {
    this();
    message = String.valueOf(index);
  }

  /**
   * Constructor for debugging
   */
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
      "\nmessage=" + message +
      '}';
  }

  /**
   * Returns the type of this event as a {@link String}: for usage with associative arrays
   *
   * @return Type as String
   */
  public String getTypeAsKey() {
    return String.valueOf(type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, eventTime);
  }
}
