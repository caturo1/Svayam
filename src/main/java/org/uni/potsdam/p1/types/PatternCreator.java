package org.uni.potsdam.p1.types;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;

//TODO create a pattern factory that transforms regex into event patterns
public final class PatternCreator {

  private PatternCreator() {
  }

  public static Pattern<Measurement, ?> seq(int first, int second, int third) {
    return Pattern.<Measurement>begin("start")
      .where(SimpleCondition.of(meas1 -> meas1.machineId == first))
      .followedBy("middle")
      .where(SimpleCondition.of(meas2 -> meas2.machineId == second))
      .followedBy("end")
      .where(SimpleCondition.of(meas3 -> meas3.machineId == third));
  }

  public static Pattern<Measurement, ?>
  seq(int withinMillis, int first, int second, int third) {
    return seq(first, second, third).within(Duration.ofMillis(withinMillis));
  }

}
