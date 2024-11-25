package org.uni.potsdam.p1.actors.processors;

import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;

import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * This class should be used to process event patterns of the ID type. Event types specified
 * as parameters are to be detected and forwarded directly to the specified operators.
 * </p>
 * <p>
 * This can be used either as an identity (all input types are selected) or filter (only
 * some input types are selected) operation.
 * </p>
 *
 * @see EventPattern
 */
public class IdFSMProcessor extends FSMProcessor {
  Set<Integer> toDetect;

  /**
   * Constructs a new ID FSMProcessor. Events determined as parameters are stored in a
   * set which is used to detect this processor's pattern.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param parameters Event types to be identified.
   */
  public IdFSMProcessor(int outputType, int... parameters) {
    super(outputType);
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
  public IdFSMProcessor(EventPattern eventPattern) {
    this(Integer.parseInt(eventPattern.name), eventPattern.getParameters());
  }

  /**
   * Detects a pattern by simply checking if the given value belongs in the event-set of
   * this instance. If so, then this event is forwarded to the next operator downstream.
   *
   * @param value New {@link Measurement} event to be processed
   * @return value if its event type is part of the set of event's of this processor.
   */
  @Override
  public Measurement processElement(Measurement value) {
    if (toDetect.contains(value.type)) {
      return value;
    }
    return null;
  }

  @Override
  public boolean applyStartCondition(int type) {
    return toDetect.contains(type);
  }
}
