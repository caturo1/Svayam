package org.uni.potsdam.p1.actors.processors;

import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.FSM;

import java.util.Set;

/**
 * This class should be used to process event patterns of the SEQ type. Event types specified
 * as parameters are to be detected together in a specified order inside the specified time window.
 *
 * @see EventPattern
 */
public class SeqFSMProcessor extends FSMProcessor {
  public String startType;

  /**
   * Constructs a new SEQ FSMProcessor. Events determined as parameters are stored in an
   * array to be used to create new FSMs as well as advancing their state.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param timeWindow Time difference between the first and last events of the pattern
   * @param parameters Event types to be identified.
   */
  public SeqFSMProcessor(String outputType, int timeWindow, String... parameters) {
    super(outputType, timeWindow, parameters);
    this.startType = parameters[0];
  }

  /**
   * Constructs a new FSMProcessor based on the information of an event pattern.
   *
   * @param eventPattern Object containing the information of the pattern to be detected.
   */
  public SeqFSMProcessor(EventPattern eventPattern) {
    this(eventPattern.name, eventPattern.timeWindow, eventPattern.getParameters());
  }

  @Override
  public boolean applyStartCondition(String type) {
    return type.equals(startType);
  }

  @Override
  public FSM getNewFSM(Event value) {
    return new SeqFSM(value);
  }

  /**
   * Finite State Machine used for the SEQ-operator. Instances keep record of already seen
   * events using an index with which they can access the parameters-array of the overlying
   * processor class. Once an FSM reaches the end of the array a new match is produced.
   */
  private class SeqFSM extends FSM {
    public int index;

    /**
     * Constructs a new FSM and set the access-index to the second position in the parameter-array
     *
     * @param value Initiator value
     */
    public SeqFSM(Event value) {
      super(value);
      index = 1;
    }

    /**
     * Constructs a new FSM using information from another instance.
     *
     * @param index        Index to be set in the new instance
     * @param participants Set of {@link Event} events to be included
     * @param startTime    Time of the oldest event in the FSM
     */
    public SeqFSM(int index, Set<Event> participants, long startTime) {
      super(participants, startTime);
      this.index = index;
    }

    @Override
    public boolean advancesWith(String type) {
      return type.equals(parameters[index]);
    }

    @Override
    public SeqFSM advancedFSM(Event value) {
      return new SeqFSM(this.index + 1, cloneAndExpandSet(value), this.startTime);
    }

    @Override
    public boolean finishesInOne() {
      return (index + 1) == parameters.length;
    }
  }
}
