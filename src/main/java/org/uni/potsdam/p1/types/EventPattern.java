package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.Objects;

/**
 * <p>
 * This class represents a complex event pattern being detected by an operator.
 * It contains:
 * </p>
 * <ul>
 *   <li>The pattern's name (The symbol representing this pattern)</li>
 *   <li>The pattern's type. Including:
 *   <ul>
 *     <li>AND = Events are detected together in any order</li>
 *     <li>SEQ = Events are detected in a specific sequential order</li>
 *     <li>OR = At least one event is detected</li>
 *   </ul>
 *   </li>
 *   <li>The list of OPERATORS to which the complex event generated by the detection of this
 *   pattern is sent</li>
 * </ul>
 */
public class EventPattern implements Serializable {
  public String name;
  public String type;
  public String[] downstreamOperators;
  boolean wasVisited;

  /**
   * Necessary empty constructor for Flink-serialization
   */
  public EventPattern() {
  }

  /**
   * Alternative way to create a new EventPattern instance. Determining the type of a
   * pattern follows the same rules as described
   * in the methods: {@link EventPattern#AND(String, String, String...)},
   * {@link EventPattern#OR(String, String, String)} and {@link EventPattern#SEQ(String, String, String)}
   *
   * @param name                The name/symbol of the complex event type produced by the detection of this pattern
   * @param type                The type of pattern
   * @param downstreamOperators The names of the downstream operators
   */
  private EventPattern(String name, String type, String... downstreamOperators) {
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

  // TODO to be improved
  private void visit() {
    wasVisited = true;
  }

  // TODO to be improved
  private void clear() {
    wasVisited = false;
  }

  /**
   * Generates an EventPattern of type AND (all events must be detected in any order).
   *
   * @param name                Name of the complex event generated by the detection of this pattern.
   * @param parameters          Types of events to be detected. Separate event types using the
   *                            symbol :
   *                            <br>
   *                            Example: 1:2:3 (events 1, 2 and 3 must be detected)
   * @param downstreamOperators Name of OPERATORS to which the complex event
   *                            generated by the detection of this pattern is sent
   * @return An EventPattern object containing the given information
   */
  public static EventPattern AND(String name, String parameters, String... downstreamOperators) {
    return new EventPattern(name, "AND:" + parameters, downstreamOperators);
  }

  /**
   * Generates an EventPattern of type SEQ (all events must be detected in a given order).
   *
   * @param name                Name of the complex event generated by the detection of this pattern.
   * @param parameters          Types of events to be detected. Separate event types using the
   *                            symbol : and indicate how many events of the same type appear in
   *                            the pattern using |
   *                            <br>
   *                            Example: 0|2:1|1 (event 0 appears 2 times and event 1 only once)
   * @param downstreamOperators Name of OPERATORS to which the complex event
   *                            generated by the detection of this pattern is sent
   * @return An EventPattern object containing the given information
   */
  public static EventPattern SEQ(String name, String parameters, String downstreamOperators) {
    return new EventPattern(name, "SEQ:" + parameters, downstreamOperators);
  }

  /**
   * Generates an EventPattern of type OR (at least one event must be detected).
   *
   * @param name                Name of the complex event generated by the detection of this pattern.
   * @param parameters          Types of events to be detected. Separate event types using the
   *                            symbol :
   *                            <br>
   *                            Example: 0:1 (event 0 or 1 must be detected)
   * @param downstreamOperators Name of OPERATORS to which the complex event
   *                            generated by the detection of this pattern is sent
   * @return An EventPattern object containing the given information
   */
  public static EventPattern OR(String name, String parameters, String downstreamOperators) {
    return new EventPattern(name, "OR:" + parameters, downstreamOperators);
  }
}
