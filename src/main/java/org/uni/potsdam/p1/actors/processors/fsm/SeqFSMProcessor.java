package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.FSM;
import org.uni.potsdam.p1.types.Measurement;

import java.util.Set;

/**
 * This class should be used to process event patterns of the SEQ type. Event types specified
 * as parameters are to be detected together in a specified order inside the specified time window.
 *
 * @see EventPattern
 */
public class SeqFSMProcessor extends FSMProcessor {
  public int startType;

  /**
   * Constructs a new SEQ FSMProcessor. Events determined as parameters are stored in an
   * array to be used to create new FSMs as well as advancing their state.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param timeWindow Time difference between the first and last events of the pattern
   * @param parameters Event types to be identified.
   */
  public SeqFSMProcessor(int outputType, int timeWindow, int... parameters) {
    super(outputType, timeWindow, parameters);
    this.startType = parameters[0];
  }

  /**
   * Constructs a new FSMProcessor based on the information of an event pattern.
   *
   * @param eventPattern Object containing the information of the pattern to be detected.
   */
  public SeqFSMProcessor(EventPattern eventPattern) {
    this(Integer.parseInt(eventPattern.name), eventPattern.timeWindow, eventPattern.getParameters());
  }

  @Override
  public boolean applyStartCondition(int type) {
    return type == startType;
  }

  @Override
  public FSM getNewFSM(Measurement value) {
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
    public SeqFSM(Measurement value) {
      super(value);
      index = 1;
    }

    /**
     * Constructs a new FSM using information from another instance.
     *
     * @param index        Index to be set in the new instance
     * @param participants Set of {@link Measurement} events to be included
     * @param startTime    Time of the oldest event in the FSM
     */
    public SeqFSM(int index, Set<Measurement> participants, long startTime) {
      super(participants, startTime);
      this.index = index;
    }

    @Override
    public boolean advancesWith(int type) {
      return type == parameters[index];
    }

    @Override
    public SeqFSM advancedFSM(Measurement value) {
      return new SeqFSM(this.index + 1, cloneAndExpandSet(value), this.startTime);
    }

    @Override
    public boolean finishesInOne() {
      return (index + 1) == parameters.length;
    }
  }
}
