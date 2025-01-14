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
  public String id;

  /**
   * Initialises a new instance of this class with a random event type [0-3].
   * @see Measurement#Measurement(int)
   */
  public Measurement() {
    type = (int) (Math.random() * 4);
    eventTime = System.currentTimeMillis();
  }

  /**
   * Constructs a new event of a given type and with a timestamp of the current system
   * clock in milliseconds.
   *
   * @param type Type of the new event
   */
  public Measurement(int type) {
    this.type = type;
    eventTime = System.currentTimeMillis();
  }

  /**
   * Constructs a new event of a given type for given predetermined event types
   *
   * @param types different possible event types
   */
  public Measurement(int[] types) {
    this(types[(int) (Math.random() * types.length)]);
  }

  /**
   * Constructs a new event of a given type in a given range
   *
   * @param lowerBound lowest event type possible
   * @param upperBound highest event type possible
   */
  public Measurement(int lowerBound, int upperBound) {
    if (upperBound < lowerBound) {
      throw new IllegalArgumentException("UpperBound must not be greater than the lower bound");
    }
    this.type = lowerBound + (int) (Math.random() * (upperBound - lowerBound));
    eventTime = System.currentTimeMillis();
  }

  public Measurement(int type, String id) {
    this(type);
    this.id = id;
  }



  @Override
  public String toString() {
    return "{\"type\":"+type + ",\"time\":" + eventTime + "}";
  }

  public String toJson(String name) {
    return "{\"type\":" + type + ",\"time\":" + eventTime + ",\"name\":\"" + name + "\",\"id\":\""+id+"\"}";
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
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Measurement that = (Measurement) o;
    return type == that.type && eventTime == that.eventTime && Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, eventTime, id);
  }
}
