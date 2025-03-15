package org.uni.potsdam.p1.actors.processors;

import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.FSM;

import java.io.Serializable;
import java.util.*;

/**
 * <p>
 * This class represents a basic Pattern processor to be used by a complex event operator.
 * FSMProcessors are responsible for maintaining and updating a list of Finite State Machines
 * {@link FSM} and using them to detect event patterns.
 * </p>
 * <p>
 * Each concrete extension of this class should * also contain a private extension of the
 * {@link FSM} class, adjusting the work logic of the finite state machines to the
 * operating logic of the corresponding processor.
 * </p>
 * <p>
 * This class can also be used to create composed event pattern processors. {@link ComposedFSMProcessor}
 * </p>
 */
public abstract class FSMProcessor implements Serializable {
  public LinkedHashSet<FSM> currentFSMs;
  public String patternType;
  public String[] parameters;
  public int timeWindow;

  /**
   * Constructs a basic FSMProcessor, only specifying the output type which represents the
   * detected pattern.
   *
   * @param patternType Number used to identify the complex event pattern to be detected.
   */
  public FSMProcessor(String patternType) {
    this.patternType = patternType;
  }

  /**
   * Constructs a new processor, specifying the output type used to identify the detected
   * pattern, the timeWindow in which events of the pattern are to be detected in and
   * the parameters (the number used to identify the input event types that are to be
   * analysed).
   *
   * @param patternType Number used to identify the complex event pattern to be detected.
   * @param timeWindow  Time difference between the first and last events of the pattern
   * @param parameters  Event types to be identified.
   */
  public FSMProcessor(String patternType, int timeWindow, String... parameters) {
    this.parameters = parameters;
    this.patternType = patternType;
    currentFSMs = new LinkedHashSet<>(5000);
    this.timeWindow = timeWindow;
  }

  /**
   * <p>
   * Iterates through the list of Finite State Machines maintained by this processor and
   * updates it accordingly using the new {@link Event} value. New {@link FSM}
   * instances are created and added to the list if this value advances the state of any
   * state machine already created and older state machines are removed from the list (in
   * accordance to {@link FSM#startsBefore(Event, int)}.
   * </p>
   * <p>
   * If this values leads to a match it will produce a new {@link Event} event of
   * the type specified by this processor and will then delete all finite state machines
   * in the list which contain at least one of the events present in the FSM that just
   * matched.
   * </p>
   *
   * @param value New {@link Event} event to be processed
   * @return A new event in case of a match or null if no match was possible.
   */
  public Event processElement(Event value) {
    Collection<FSM> candidates = new ArrayList<>(100);
    Collection<FSM> toDelete = new ArrayList<>();

    boolean isFirst = true;
    FSM firstFound = null;
    for (FSM current : currentFSMs) {
      if (applyTimeBoundary(current, value)) {
        toDelete.add(current);
        continue;
      }
      if (current.advancesWith(value.type)) {
        if (current.finishesInOne()) {
          StringBuilder outputId = new StringBuilder(current.participants.size() + 1);
          for (Event meas : current.participants) {
            outputId.append(meas.id).append("-");
          }
          outputId.append(value.id);
          currentFSMs.removeIf(fsm -> {
            if (fsm == current) {
              return false;
            }
            return !Collections.disjoint(fsm.participants,current.participants);
          });
          currentFSMs.remove(current);
          toDelete.forEach(currentFSMs::remove);
          return new Event(patternType, outputId.toString());
        }
        candidates.add(getNextFSM(current, value));
        if (isFirst) {
        firstFound = current;
          isFirst = false;
        }
      }
    }
    if(firstFound!=null){toDelete.add(firstFound);}
    if (applyStartCondition(value.type)) {
      currentFSMs.add(getNewFSM(value));
    }
    toDelete.forEach(currentFSMs::remove);
    currentFSMs.addAll(candidates);
    return null;
  }

  /**
   * Determines which types can initiate a new Finite State Machine
   *
   * @param type Initiator type
   * @return True, if this type meet a specified condition.
   */
  public boolean applyStartCondition(String type) {
    return false;
  }

  /**
   * Checks if a finite state machine is too old to match with a new event.
   *
   * @param current The finite state machine
   * @param value   The measurement event
   * @return False, if current is not old enough or the specified time windows is bellow zero
   * (no time window specified)
   */
  public boolean applyTimeBoundary(FSM current, Event value) {
    if (this.timeWindow <= 0) {
      return false;
    }
    return current.startsBefore(value, timeWindow);
  }

  /**
   * Creates a new finite state machine for a given {@link Event} event. To be
   * specified in each extension of this class.
   *
   * @param value New initiator event.
   * @return New Finite State Machine
   */
  public FSM getNewFSM(Event value) {
    return null;
  }

  /**
   * Uses a new {@link Event} event to advance the state of a given finite state machine.
   * Creates a new machine in this advanced state and returns it.
   *
   * @param current Given finite state machine.
   * @param value   Event for the state transition.
   * @return New finite state machine in a new advanced state.
   */
  public FSM getNextFSM(FSM current, Event value) {
    return current.advancedFSM(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FSMProcessor that = (FSMProcessor) o;
    return Objects.equals(patternType, that.patternType) && Objects.equals(currentFSMs, that.currentFSMs) && Objects.deepEquals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentFSMs, patternType, Arrays.hashCode(parameters));
  }

  /**
   * Creates a new composed FSMProcessor which only creates outputs if a given measurement
   * event leads to matches in all underlying processors.
   *
   * @param outputType Identifying number for the composed FSMProcessor
   * @param processor1 First processor. Can be a ComposedFSMProcessor or null
   * @param processor2 Second processor. Can be a ComposedFSMProcessor or null
   * @return A new composed event processor.
   */
  public static ComposedFSMProcessor composeAND(String outputType, FSMProcessor processor1, FSMProcessor processor2) {
    return new ComposedFSMProcessor(outputType, processor1, processor2) {
      @Override
      public boolean applyAcceptanceCondition(Event result1, Event result2) {
        return result1 != null && result2 != null;
      }
    };
  }

  /**
   * Creates a new composed FSMProcessor which creates outputs if a given measurement
   * event leads to matches in at least one underlying processor.
   *
   * @param outputType Identifying number for the composed FSMProcessor
   * @param processor1 First processor. Can also be a ComposedFSMProcessor or null
   * @param processor2 Second processor. Can also be a ComposedFSMProcessor or null
   * @return A new composed event processor.
   */
  public static ComposedFSMProcessor composeOR(String outputType, FSMProcessor processor1, FSMProcessor processor2) {
    return new ComposedFSMProcessor(outputType, processor1, processor2) {
      @Override
      public boolean applyAcceptanceCondition(Event result1, Event result2) {
        return result1 != null || result2 != null;
      }
    };
  }
}
