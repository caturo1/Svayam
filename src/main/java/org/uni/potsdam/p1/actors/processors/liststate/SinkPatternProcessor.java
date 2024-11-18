package org.uni.potsdam.p1.actors.processors.liststate;

import org.apache.flink.api.common.state.ListState;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.Measurement;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

/**
 * Simple pattern processor implementing the AND pattern for two different vent types coming
 * from the operators downstream.
 *
 * @see AndPatternProcessor for more information
 */
@Deprecated
public class SinkPatternProcessor extends PatternProcessor {

  ListState<Measurement> listState;
  ListState<Measurement> listState2;

  /**
   * Initialises list states alongside the usual parameters for a {@link PatternProcessor}.
   *
   * @see PatternProcessor#PatternProcessor(Measurement, String, Measurer)
   */
  public SinkPatternProcessor(Measurement value, String outputType, ListState<Measurement> listState1, ListState<Measurement> listState2, Measurer<LocalTime> measurer) {
    super(value, outputType, measurer);
    this.listState = listState1;
    this.listState2 = listState2;
  }

  @Override
  public Optional<Measurement> process() throws Exception {
    Optional<Measurement> result = Optional.empty();
    List<Measurement> listo = (List<Measurement>) listState.get();
    if (!listo.isEmpty()) {
      Measurement current = listo.get(0);
      result = Optional.of(new Measurement(Integer.parseInt(outputType), String.format("%s%n%s%n", value, current), 1));
      measurer.update(outputType);
      listState.update(listo.subList(1, listo.size()));
    } else {
      listState2.add(value);
    }
    return result;
  }
}
