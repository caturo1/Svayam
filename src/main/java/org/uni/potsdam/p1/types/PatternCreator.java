package org.uni.potsdam.p1.types;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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

  public static Pattern<Measurement, ?>
  lazySeq(int withinMillis) {
    return lazySeq().within(Duration.ofMillis(withinMillis));
  }

  //TODO varargs parameter describing the events to be detected - collect them in a set
  // or collection and create the pattern incrementally in a while loop
  public static Pattern<Measurement, ?> lazySeq() {

    return Pattern.begin(
      Pattern.<Measurement>begin("start")
        .where(SimpleCondition.of(meas -> meas.machineId > 0))
        .followedBy("middle")
        .where(new IterativeCondition<>() {
          @Override
          public boolean filter(Measurement value, Context<Measurement> ctx) throws Exception {
            return value.machineId > 0 &&
              value.machineId !=
                ctx.getEventsForPattern("start").iterator().next().machineId;
          }
        })
        .followedBy("end")
        .where(new IterativeCondition<>() {
          @Override
          public boolean filter(Measurement value, Context<Measurement> ctx) throws Exception {
            Measurement firstEvent = ctx.getEventsForPattern("start").iterator().next();
            Measurement secondEvent = ctx.getEventsForPattern("middle").iterator().next();
            return !(value.machineId == 0 ||
              firstEvent.machineId == value.machineId ||
              secondEvent.machineId == value.machineId);
          }
        })
    );
  }
}
