package org.uni.potsdam.p1.actors.processors;

import org.uni.potsdam.p1.types.Event;

/**
 * This class is a special kind of {@link FSMProcessor} used to create complex patterns.
 * It can be used to chain multiple processors together, so that they all process the same
 * event for their own set of individual Finite State Machines.
 *
 * @see FSMProcessor#composeAND(String, FSMProcessor, FSMProcessor)
 * @see FSMProcessor#composeOR(String, FSMProcessor, FSMProcessor)
 */
public abstract class ComposedFSMProcessor extends FSMProcessor {
  FSMProcessor processor1;
  FSMProcessor processor2;

  /**
   * Creates a composed FSMProcessor by grouping together 2 different FSMProcessors.
   * If those processors are ComposedFSMProcessor it is possible to create processors
   * for complex patterns. Using null as an argument allows the user to create branches
   * with a single processor.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param processor1 First processor. Can be a ComposedFSMProcessor or null
   * @param processor2 Second processor. Can be a ComposedFSMProcessor or null
   */
  public ComposedFSMProcessor(String outputType, FSMProcessor processor1, FSMProcessor processor2) {
    super(outputType);
    this.processor1 = processor1;
    this.processor2 = processor2;
  }

  /**
   * For all available non-null processors, process the given element. If the acceptance
   * condition is met, then a new Event event with the type of this processor's
   * pattern is generated.
   *
   * @param value New {@link Event} event to be processed
   * @return {@link Event} event of this pattern's type or null, if the condition wasn't met.
   */
  @Override
  public Event processElement(Event value) {
    Event result1 = getResult(processor1, value);
    Event result2 = getResult(processor2, value);
    if (applyAcceptanceCondition(result1, result2)) {
      return new Event(patternType);
    }
    return null;
  }

  public abstract boolean applyAcceptanceCondition(Event result1, Event result2);

  /**
   * Gets the result of an underlying processor if it is not null.
   *
   * @param processor Given processor.
   * @param value     Event to be processed.
   * @return A new Event or null.
   */
  public Event getResult(FSMProcessor processor, Event value) {
    if (processor == null) {
      return null;
    }
    return processor.processElement(value);
  }
}
