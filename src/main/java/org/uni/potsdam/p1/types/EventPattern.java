package org.uni.potsdam.p1.types;

import org.uni.potsdam.p1.actors.processors.AndFSMProcessor;
import org.uni.potsdam.p1.actors.processors.FSMProcessor;
import org.uni.potsdam.p1.actors.processors.OrFSMProcessor;
import org.uni.potsdam.p1.actors.processors.SeqFSMProcessor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public int timeWindow;
  public Map<String, Integer> weightMap;

  /**
   * Necessary empty constructor for Flink-serialization
   */
  public EventPattern() {
  }

  /**
   * Alternative way to create a new EventPattern instance. Determining the type of
   * pattern follows the same rules as described
   * in the methods: {@link EventPattern#AND(String, String, int, String...)},
   * {@link EventPattern#OR(String, String, int, String...)} and  {@link EventPattern#SEQ(String, String, int, String...)}
   *
   * @param name                The name/symbol of the complex event type produced by the detection of this pattern
   * @param type                The type of pattern
   * @param downstreamOperators The names of the downstream operators
   */
  private EventPattern(String name, String type, String... downstreamOperators) {
    this.name = name;
    this.type = type;
    this.downstreamOperators = downstreamOperators;
    getWeightMaps();
  }

  /**
   * Alternative constructor defining basic characteristics of an EventPattern.
   *
   * @param name                Identifying name for this pattern.
   * @param type                Pattern's type (AND, SEQ, OR)
   * @param timeWindow          Maximal amount of time between the first and last events in this pattern.
   * @param downstreamOperators Operators that receive this pattern upon its detection.
   */
  private EventPattern(String name, String type, int timeWindow, String... downstreamOperators) {
    this(name, type, downstreamOperators);
    this.timeWindow = timeWindow;
  }

  public String getType() {
    return type.substring(0, type.indexOf(":"));
  }

  public String getName() {
    return name;
  }

  /**
   * Parses the list of parameters from the given list.
   *
   * @return Parameters stored in the same order in which they were first written in this
   * object's constructor-call.
   */
  public int[] getParameters() {
    int separator = type.indexOf(":");
    String kind = type.substring(0, separator);
    String[] split = type.substring(separator + 1).split(":");
    if (kind.matches("(AND|OR)")) {
      return Arrays.stream(split).mapToInt(Integer::parseInt).toArray();
    } else if (kind.equals("SEQ")) {
      return Arrays.stream(split).flatMapToInt(paramater -> {
        String[] division = paramater.split("\\|");
        int value = Integer.parseInt(division[0]);
        int quantity = Integer.parseInt(division[1]);
        return IntStream.generate(() -> value).limit(quantity);
      }).toArray();
    }
    throw new IllegalArgumentException("False kind of EventPattern type given. Please use AND, SEQ or OR.");
  }

  /**
   * Returns a map with the weights assigned to each parameter. In AND or OR patterns this
   * will always be 1. In SEQ patterns this will be defined during the pattern creation
   * for each individual parameter. It corresponds to the amount of events pro event
   * type in this pattern.
   *
   * @return The map object linking the input types to their frequency in the pattern.
   */
  public Map<String, Integer> getWeightMaps() {
    if (weightMap == null) {
      int separator = type.indexOf(":");
      String kind = type.substring(0, separator);
      String[] split = type.substring(separator + 1).split(":");
      if (kind.matches("(AND|OR)")) {
        weightMap = Arrays.stream(split).distinct().collect(Collectors.toMap(
          value -> value, value -> 1));
      } else if (kind.equals("SEQ")) {
        weightMap = Arrays.stream(split)
          .collect(Collectors.groupingBy(
              (String string) -> string.substring(0, string.indexOf("|")),
              Collectors.mapping(
                (String string) -> Integer.parseInt(string.substring(string.indexOf("|") + 1)),
                Collectors.reducing(0, Integer::sum)
              )
            )
          );
      } else {
        throw new IllegalArgumentException("False kind of EventPattern type given. Please use AND, SEQ or OR.");
      }
    }
    return weightMap;
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
  public static EventPattern AND(String name, String parameters, int timeWindow, String... downstreamOperators) {
    return new EventPattern(name, "AND:" + parameters, timeWindow, downstreamOperators);
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
  public static EventPattern SEQ(String name, String parameters, int timeWindow, String... downstreamOperators) {
    return new EventPattern(name, "SEQ:" + parameters, timeWindow, downstreamOperators);
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
  public static EventPattern OR(String name, String parameters, int timeWindow, String... downstreamOperators) {
    return new EventPattern(name, "OR:" + parameters, timeWindow, downstreamOperators);
  }

  /**
   * Creates a new {@link FSMProcessor} in accordance to the type of the given patterns
   * and add them to the given processors-map.
   *
   * @param processors Map of FSM processors of an OperatorGraph
   * @param patterns   Patterns of an operator.
   */
  public static void addProcessors(Map<String, FSMProcessor> processors, EventPattern[] patterns) {
    for (EventPattern pattern : patterns) {
      switch (pattern.getType()) {
        case "AND": {
          processors.put(pattern.name, new AndFSMProcessor(pattern));
          break;
        }
        case "OR": {
          processors.put(pattern.name, new OrFSMProcessor(pattern));
          break;
        }
        case "SEQ": {
          processors.put(pattern.name, new SeqFSMProcessor(pattern));
        }
      }
    }
  }
}
