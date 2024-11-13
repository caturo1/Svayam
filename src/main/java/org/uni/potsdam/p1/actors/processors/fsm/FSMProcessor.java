package org.uni.potsdam.p1.actors.processors.fsm;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.FSM;
import org.uni.potsdam.p1.types.Measurement;

import java.io.Serializable;
import java.util.*;

public abstract class FSMProcessor implements Serializable {
  public List<FSM> currentFSMs;
  public int patternType;
  public int[] pattern;
  public int timeWindow;

  public FSMProcessor(int patternType) {
    this.patternType = patternType;
  }

  public FSMProcessor(int outputType, int timeWindow, int... pattern) {
    this.pattern = pattern;
    this.patternType = outputType;
    currentFSMs = new ArrayList<>();
    this.timeWindow = timeWindow;
  }

  public Measurement processElement(Measurement value) {
    List<FSM> candidates = new ArrayList<>();
    List<FSM> toDelete = new ArrayList<>();
    for (FSM current : currentFSMs) {
      if (applyTimeBoundary(current, value)) {
        toDelete.add(current);
        continue;
      }
      if (current.advancesWith(value.type)) {
        if (current.finishesInOne()) {
          StringBuilder output = new StringBuilder();
          current.participants.forEach(output::append);
          currentFSMs.removeIf(fsm -> {
            if (fsm == current) {
              return false;
            }
            return fsm.contains(current.participants);
          });
          currentFSMs.remove(current);
          currentFSMs.remove(toDelete);
          output.append(value);
          return new Measurement(patternType, output.toString(), 0);
        }
        candidates.add(getNextFSM(current, value));
      }
    }
    if (applyStartCondition(value.type)) {
      currentFSMs.add(getNewFSM(value));
    }
    currentFSMs.removeAll(toDelete);
    currentFSMs.addAll(candidates);
    return null;
  }

  public boolean applyStartCondition(int type) {
    return false;
  }

  public boolean applyTimeBoundary(FSM current, Measurement value) {
    if (this.timeWindow <= 0) {
      return false;
    }
    return current.startsBefore(value, 10);
  }

  public FSM getNewFSM(Measurement value) {
    return null;
  }

  public FSM getNextFSM(FSM current, Measurement value) {
    return current.advancedFSM(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FSMProcessor that = (FSMProcessor) o;
    return patternType == that.patternType && Objects.equals(currentFSMs, that.currentFSMs) && Objects.deepEquals(pattern, that.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentFSMs, patternType, Arrays.hashCode(pattern));
  }

  public static ProcessFunction<Measurement, Measurement> createProcessFunction(FSMProcessor processor) {
    return new ProcessFunction<>() {

      @Override
      public void processElement(Measurement value, ProcessFunction<Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
        Optional.ofNullable(processor.processElement(value)).ifPresent(out::collect);
      }
    };
  }

  public static ComposeFSMProcessor composeAND(int outputType, FSMProcessor processor1, FSMProcessor processor2) {
    return new ComposeFSMProcessor(outputType, processor1, processor2) {
      @Override
      public boolean applyAcceptanceCondition(Measurement result1, Measurement result2) {
        return result1 != null && result2 != null;
      }
    };
  }

  public static ComposeFSMProcessor composeOR(int outputType, FSMProcessor processor1, FSMProcessor processor2) {
    return new ComposeFSMProcessor(outputType, processor1, processor2) {
      @Override
      public boolean applyAcceptanceCondition(Measurement result1, Measurement result2) {
        return result1 != null || result2 != null;
      }
    };
  }
}
