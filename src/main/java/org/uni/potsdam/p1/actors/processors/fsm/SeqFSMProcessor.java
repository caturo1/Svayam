package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.FSM;
import org.uni.potsdam.p1.types.Measurement;

import java.util.Set;

public class SeqFSMProcessor extends FSMProcessor {
  public int startType;

  public SeqFSMProcessor(int outputType, int timeWindow, int... pattern) {
    super(outputType, timeWindow, pattern);
    this.startType = pattern[0];
  }

  @Override
  public boolean applyStartCondition(int type) {
    return type == startType;
  }

  @Override
  public FSM getNewFSM(Measurement value) {
    return new SeqFSM(value);
  }

  private class SeqFSM extends FSM {
    public int index;

    public SeqFSM(Measurement value) {
      super(value);
      index = 1;
    }

    public SeqFSM(int index, Set<Measurement> participants, long startTime) {
      super(participants, startTime);
      this.index = index;
    }

    @Override
    public boolean advancesWith(int type) {
      return type == pattern[index];
    }

    @Override
    public SeqFSM advancedFSM(Measurement value) {
      return new SeqFSM(this.index + 1, cloneAndExpandSet(value), this.startTime);
    }

    @Override
    public boolean finishesInOne() {
      return (index + 1) == pattern.length;
    }
  }
}
