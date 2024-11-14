package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;

import java.util.HashSet;
import java.util.Set;

/**
 * This class should be used to process event patterns of the OR type. Event types specified
 * as parameters are to be detected together in any order inside the specified time window.
 *
 * @see EventPattern
 */
public class OrFSMProcessor extends FSMProcessor {

  Set<Integer> toDetect;

  /**
   * Constructs a new OR FSMProcessor. Events determined as parameters are stored in a
   * set which is used to detect this processor's pattern.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param timeWindow Time difference between the first and last events of the pattern
   * @param parameters Event types to be identified.
   */
  public OrFSMProcessor(int outputType, int timeWindow, int... parameters) {
    super(outputType, timeWindow, parameters);
    toDetect = new HashSet<>(parameters.length);
    for (int j : parameters) {
      toDetect.add(j);
    }
  }

  /**
   * Constructs a new FSMProcessor based on the information of an event pattern.
   *
   * @param eventPattern Object containing the information of the pattern to be detected.
   */
  public OrFSMProcessor(EventPattern eventPattern) {
    this(Integer.parseInt(eventPattern.name), eventPattern.timeWindow, eventPattern.getParameters());
  }

  /**
   * Detects a pattern by simply checking if the given value belongs in the event-set of
   * this instance
   *
   * @param value New {@link Measurement} event to be processed
   * @return A new event in case of a match or null if no match was possible.
   */
  @Override
  public Measurement processElement(Measurement value) {
    if (toDetect.contains(value.type)) {
      return new Measurement(patternType, value.toString(), 0);
    }
    return null;
  }

  @Override
  public boolean applyStartCondition(int type) {
    return toDetect.contains(type);
  }
}
