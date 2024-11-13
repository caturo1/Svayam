package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.FSM;
import org.uni.potsdam.p1.types.Measurement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AndFSMProcessor extends FSMProcessor {

  Map<Integer, Integer> toDetect;

  public AndFSMProcessor(int outputType, int timeWindow, int... pattern) {
    super(outputType, timeWindow, pattern);
    toDetect = new HashMap<>(pattern.length);
    for (int i = 0; i < pattern.length; i++) {
      toDetect.put(pattern[i], i);
    }
  }

  @Override
  public boolean applyStartCondition(int type) {
    return toDetect.containsKey(type);
  }

  @Override
  public FSM getNewFSM(Measurement value) {
    return new AndFSM(value);
  }

  private class AndFSM extends FSM {
    boolean[] containType;

    public AndFSM(Measurement value) {
      super(value);
      containType = new boolean[pattern.length];
      containType[toDetect.get(value.type)] = true;
    }

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
      return participants.size() + 1 == pattern.length;
    }
  }
}
