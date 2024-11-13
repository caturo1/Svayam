package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.Measurement;

import java.util.HashSet;
import java.util.Set;

public class OrFSMProcessor extends FSMProcessor {

  Set<Integer> toDetect;

  public OrFSMProcessor(int outputType, int timeWindow, int... pattern) {
    super(outputType, timeWindow, pattern);
    toDetect = new HashSet<>(pattern.length);
    for (int j : pattern) {
      toDetect.add(j);
    }
  }

  @Override
  public Measurement processElement(Measurement value) {
    if (toDetect.contains(value.type)) {
      return new Measurement(patternType, value.toString(), 0);
    }
    return null;
  }

  @Override
  public boolean applyStartCondition(int type) {
    return toDetect.contains(type);
  }

}
