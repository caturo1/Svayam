package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.FSM;
import org.uni.potsdam.p1.types.Measurement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class should be used to process event patterns of the AND type. Event types specified
 * as parameters are to be detected together in any order inside the specified time window.
 *
 * @see EventPattern
 */
public class AndFSMProcessor extends FSMProcessor {

  Map<Integer, Integer> toDetect;

  /**
   * Constructs a new AND FSMProcessor. Events determined as parameters are stored in a
   * set and used to create new FSMs as well advancing their state.
   *
   * @param outputType Number used to identify the complex event pattern to be detected.
   * @param timeWindow Time difference between the first and last events of the pattern
   * @param parameters Event types to be identified.
   */
  public AndFSMProcessor(int outputType, int timeWindow, int... parameters) {
    super(outputType, timeWindow, parameters);
    toDetect = new HashMap<>(parameters.length);
    for (int i = 0; i < parameters.length; i++) {
      toDetect.put(parameters[i], i);
    }
  }

  /**
   * Constructs a new FSMProcessor based on the information of an event pattern.
   *
   * @param eventPattern Object containing the information of the pattern to be detected.
   */
  public AndFSMProcessor(EventPattern eventPattern) {
    this(Integer.parseInt(eventPattern.name), eventPattern.timeWindow, eventPattern.getParameters());
  }

  @Override
  public boolean applyStartCondition(int type) {
    return toDetect.containsKey(type);
  }

  @Override
  public FSM getNewFSM(Measurement value) {
    return new AndFSM(value);
  }

  /**
   * Finite State Machine used for the AND-operator. Instances keep record of already seen
   * events using a boolean array as a checklist. Once the array is completely true a new
   * event is generated.
   */
  private class AndFSM extends FSM {
    boolean[] containType;

    /**
     * Constructs a new FSM and set the boolean value correspondent to the given measurement event as true.
     *
     * @param value Initiator value
     */
    public AndFSM(Measurement value) {
      super(value);
      containType = new boolean[parameters.length];
      containType[toDetect.get(value.type)] = true;
    }

    /**
     * Constructs a new FSM using information from another instance.
     *
     * @param index        Index of the boolean value to be set to true in the new instance.
     * @param types        Array of event types already included in the FSM.
     * @param participants Set of {@link Measurement} events to be included.
     * @param startTime    Time of the oldest event in the FSM
     */
    public AndFSM(int index, boolean[] types, Set<Measurement> participants, long startTime) {
      super(participants, startTime);
      containType = Arrays.copyOf(types, types.length);
      containType[index] = true;
    }

    @Override
    public boolean advancesWith(int type) {
      int index = toDetect.getOrDefault(type, -1);
      return index != -1 && !containType[index];
    }

    @Override
    public AndFSM advancedFSM(Measurement value) {
      return new AndFSM(toDetect.get(value.type), this.containType, cloneAndExpandSet(value), this.startTime);
    }

    @Override
    public boolean finishesInOne() {
      return participants.size() + 1 == parameters.length;
    }
  }
}
