package org.uni.potsdam.p1.types;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FSM {
  public Set<Measurement> participants;
  public long startTime;

  public FSM(Measurement value) {
    this.participants = new HashSet<>(List.of(value));
    this.startTime = value.eventTime;
  }

  public FSM(Set<Measurement> participants, long startTime) {
    this.participants = participants;
    this.startTime = startTime;
  }

  public FSM advancedFSM(Measurement value) {
    return new FSM(value);
  }

  public boolean startsBefore(Measurement value, int timeInSeconds) {
    return startTime < value.eventTime - TimeUnit.SECONDS.toMillis(timeInSeconds);
  }

  public boolean advancesWith(int type) {
    return false;
  }

  public boolean finishesInOne() {
    return false;
  }

  public boolean contains(Measurement measurement) {
    return participants.contains(measurement);
  }

  public boolean contains(Set<Measurement> measurement) {
    return participants.removeAll(measurement);
  }

  public Set<Measurement> cloneAndExpandSet(Measurement value) {
    Set<Measurement> newSet = new HashSet<>(this.participants);
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
