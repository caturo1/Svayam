package org.uni.potsdam.p1.types;

import org.uni.potsdam.p1.actors.processors.FSMProcessor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class represents a basic Finite State Machine. It only implements basic logic
 * for constructing and controlling the state of a machine as well as information about
 * the {@link Event} instances used for its transitions. Extensions of this class
 * are to be used together with a {@link FSMProcessor}
 * , where they are adjusted to the behavior of the processor.
 */
public class FSM {
  public Set<Event> participants;
  public long startTime;

  /**
   * Constructs a new FSM with only a single event value.
   *
   * @param value Initiating event.
   */
  public FSM(Event value) {
    this.participants = new HashSet<>(List.of(value));
    this.startTime = value.eventTime;
  }

  /**
   * Constructs a new FSM for a given set of measurement events and a starting time
   *
   * @param participants Set of events
   * @param startTime    Time of the very first event in the machine.
   */
  public FSM(Set<Event> participants, long startTime) {
    this.participants = participants;
    this.startTime = startTime;
  }

  /**
   * Create a new Finite State Machine in an advanced state. The given value advances the
   * state of the current State Machine (this) and is thus included in the list of
   * participant-events of the new instance.
   *
   * @param value Event event that advances the state of the current machine.
   * @return New state machine in the new advanced state.
   */
  public FSM advancedFSM(Event value) {
    return new FSM(value);
  }

  /**
   * Check if the current read {@link Event} value is timeInSeconds older than the
   * first event contained in this Finite State Machine.
   *
   * @param value         Event event used for comparison.
   * @param timeInSeconds Time window for the comparison.
   * @return True if the event is older than the time difference between the event time of
   * value with timeInSeconds.
   */
  public boolean startsBefore(Event value, int timeInSeconds) {
    return startTime < value.eventTime - TimeUnit.SECONDS.toMillis(timeInSeconds);
  }

  /**
   * Checks if the given event type advances the current Finite State Machine.
   *
   * @param type Type of event
   * @return True if it advances the state of this instance.
   */
  public boolean advancesWith(String type) {
    return false;
  }

  /**
   * Checks if the current state machine only need one more transition to reach its
   * termination state. This is used in the {@link FSMProcessor}
   * classes to determine matches.
   */
  public boolean finishesInOne() {
    return false;
  }


  /**
   * Checks if the current state machine contains one or all of the Measurements specified
   * in the given set. This is used in the {@link FSMProcessor}
   * classes to remove Finite State Machines which contain already matched events.
   *
   * @param events Set of events to be found.
   * @return True if this instance contains one or more events in the set.
   */
  public boolean contains(Set<Event> events) {
    return participants.removeAll(events);
  }

  /**
   * Creates a new set of measurement events using those already contained in this instance
   * in addition to a new one. This is used in the {@link FSMProcessor}
   * to create the sets of events contained in new FSMs.
   *
   * @param value New {@link Event} event to be added
   * @return Expanded set of events.
   */
  public Set<Event> cloneAndExpandSet(Event value) {
    Set<Event> newSet = new HashSet<>(this.participants);
    newSet.add(value);
    return newSet;
  }

  @Override
  public String toString() {
    return "FSM{" +
      "participants=" + participants +
      ", startTime=" + startTime +
      '}';
  }
}
