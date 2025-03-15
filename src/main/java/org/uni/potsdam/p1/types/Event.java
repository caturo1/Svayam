package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

import static org.uni.potsdam.p1.execution.Settings.FACTOR;

/**
 * This class is the main abstraction of the DataStreamJob, representing a CPU-Event
 * from the Google-borg-cluster.
 */
public class Event implements Serializable {
  public final String type;
  public final long eventTime;
  public String id;

  /**
   * Initialises a new instance of this class with a random event type [0-3].
   * @see Event#Event(String)
   */
  public Event() {
    type = "";
    eventTime = System.currentTimeMillis();
    id = System.nanoTime()+"";
  }

  /**
   * Constructs a new event of a given type and with a timestamp of the current system
   * clock in milliseconds.
   *
   * @param type Type of the new event
   */
  public Event(String type) {
    this.type = type;
    eventTime = System.currentTimeMillis();
    id = System.nanoTime()+"";
  }
  /**
   * Constructs a new event of a given type and with a predetermined timestamp.
   *
   * @param type Type of the new event
   * @param time Timestamp of the event
   */
  public Event(String type, Instant time) {
    this.type = type;
    eventTime = time.toEpochMilli();
    id = System.nanoTime()+"";
  }

  /**
   * Constructs a new event of a given type for given predetermined event types
   *
   * @param types different possible event types
   */
  public Event(String[] types) {
    this(types[(int) (Math.random() * types.length)]);
  }


  /**
   * Constructs a new event of a given type in a given range
   *
   * @param lowerBound lowest event type possible
   * @param upperBound highest event type possible
   */
  public Event(int lowerBound, int upperBound) {
    if (upperBound < lowerBound) {
      throw new IllegalArgumentException("UpperBound must not be greater than the lower bound");
    }
    this.type = ""+lowerBound + (int) (Math.random() * (upperBound - lowerBound));
    eventTime = System.currentTimeMillis();
  }

  public Event(String type, String id) {
    this(type);
    this.id = id;
  }

  public Event(String type, String id,Instant time) {
    this.type = type;
    this.eventTime = time.plusSeconds(1).plusNanos(FACTOR).toEpochMilli();
    this.id = id;
  }


  @Override
  public String toString() {
    return "{\"type\":" + type + ",\"time\":" + eventTime + ",\"id\":\""+id+"\"}";
  }

  public String toString(String name) {
    return "{\"type\":" + type + ",\"time\":" + eventTime + ",\"name\":\"" + name + "\",\"id\":\""+id+"\"}";
  }

  /**
   * Returns the type of this event as a {@link String}: for usage with associative arrays
   *
   * @return Type as String
   */
  public String getTypeAsKey() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Event that = (Event) o;
    return type.equals(that.type) && eventTime == that.eventTime && Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, eventTime, id);
  }
}
