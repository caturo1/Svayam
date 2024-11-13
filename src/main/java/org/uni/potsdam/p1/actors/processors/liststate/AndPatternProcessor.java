package org.uni.potsdam.p1.actors.processors.liststate;

import org.apache.flink.api.common.state.ListState;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.Measurement;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * This class implements the detection of a Pattern of the form 123 for events of the
 * {@link Measurement} class and serves as a standard for the implementation of
 * PatternProcessors of this type (AND). The operator uses {@link ListState} instances to
 * keep track of incoming events of type 1,2 and 3 in an operator and stores them
 * sequentially based on their time of arrival.
 * </p>
 * <p>
 * Checks for pattern detection are done by the arrival
 * of events of any one of the types mentioned above.
 * The {@link PatternProcessor} instance will iterate through the lists of events different
 * from the one just received and check for matches.
 * </p>
 * <p>
 * The operator updates the lists after executing the pattern detection algorithm and
 * will discard any events that are 10 seconds older than the current event.
 * </p>
 */
public class AndPatternProcessor extends PatternProcessor {

  ListState<Measurement> listState;
  ListState<Measurement> listState2;
  ListState<Measurement> listState3;

  /**
   * Initialises list states alongside the usual parameters for a {@link PatternProcessor}.
   *
   * @see PatternProcessor#PatternProcessor(Measurement, String, Measurer)
   */
  public AndPatternProcessor(Measurement value, String outputType, ListState<Measurement> listState, ListState<Measurement> listState2, ListState<Measurement> listState3, Measurer<LocalTime> measurer) {
    super(value, outputType, measurer);
    this.listState = listState;
    this.listState2 = listState2;
    this.listState3 = listState3;
  }

  @Override
  public Optional<Measurement> process() throws Exception {
    Optional<Measurement> result = Optional.empty();

    List<Measurement> list2 = (List<Measurement>) listState2.get();
    List<Measurement> list3 = (List<Measurement>) listState3.get();

    if (list2 != null && list3 != null) {
      int index1 = findIndex(list2, value.eventTime);
      int index2 = findIndex(list3, value.eventTime);
      int size1 = list2.size();
      int size2 = list3.size();
      if (index1 < size1 && index2 < size2) {
        result = Optional.of(new Measurement(Integer.parseInt(outputType), String.format("%s%n%s%n%s%n", value, list2.get(index1), list3.get(index2)), 2));
        measurer.update(outputType);
        listState2.update(list2.subList(index1 + 1, size1));
        listState3.update(list3.subList(index2 + 1, size2));
      } else {
        if (index1 > 0) {
          listState2.update(list2.subList(index1, size1));
        }
        if (index2 > 0) {
          listState3.update(list3.subList(index2, size2));
        }
        listState.add(value);
      }
    }
    return result;
  }
}
