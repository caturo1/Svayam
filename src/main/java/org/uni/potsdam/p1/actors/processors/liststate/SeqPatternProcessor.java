package org.uni.potsdam.p1.actors.processors.liststate;

import org.apache.flink.api.common.state.ListState;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.Measurement;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * This class implements the detection of a Pattern of the form Seq(001) for events of the
 * {@link Measurement} class and serves as a standard for the implementation of
 * PatternProcessors of this type (SEQ). The operator uses a {@link ListState} instance to
 * keep track of incoming events of type 0 in an operator and stores them sequentially
 * based on their time of arrival.
 * </p>
 * <p>
 * Checks for pattern detection are done by the arrival
 * of events of type 1: an object of this class will then iterate through the operator's
 * list of stored 0-events until it finds a match or reach the end of the list.
 * </p>
 * <p>
 * In both cases the operator will discard all events in the list that are 10 seconds
 * older than the current event.
 * </p>
 */
public class SeqPatternProcessor extends PatternProcessor {

  ListState<Measurement> listState;

  /**
   * Initialises a list state alongside the usual parameters for a {@link PatternProcessor}.
   *
   * @see PatternProcessor#PatternProcessor(Measurement, String, Measurer)
   */
  public SeqPatternProcessor(Measurement value, String outputType, ListState<Measurement> listState, Measurer<LocalTime> measurer) {
    super(value, outputType, measurer);
    this.listState = listState;
  }

  @Override
  public Optional<Measurement> process() throws Exception {
    Optional<Measurement> result = Optional.empty();
    if (value.type == 0) {
      listState.add(value);
    } else if (value.type == 1) {
      List<Measurement> listOfZeros = (List<Measurement>) listState.get();
      int amountOfZeros = listOfZeros.size();
      if (amountOfZeros > 1) {
        int index = findIndex(listOfZeros, value.eventTime);
        if (index < amountOfZeros - 1) {
          Measurement secondCandidate = listOfZeros.get(index + 1);
          long diff = value.eventTime - secondCandidate.eventTime;
          if (diff > 0 && diff <= TIME_BOUND) {
            result = Optional.of(new Measurement(Integer.parseInt(outputType), String.format("%s%n%s%n%s%n", value, secondCandidate, listOfZeros.get(index)), 2));
            measurer.update(outputType);
            listState.update(listOfZeros.subList(index + 2, amountOfZeros));
          } else {
            listState.update(listOfZeros.subList(index, amountOfZeros));
          }
        } else {
          listState.update(listOfZeros.subList(index, amountOfZeros));
        }
      }
    }
    return result;
  }
}
