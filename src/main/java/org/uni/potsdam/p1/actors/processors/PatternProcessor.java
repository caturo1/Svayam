package org.uni.potsdam.p1.actors.processors;

import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.types.Measurement;

import java.time.LocalTime;
import java.util.List;
import java.util.Optional;

/**
 * This class represents the different pattern processors used by operators for the
 * detection of complex events.
 */
public abstract class PatternProcessor {
  Measurement value;
  String outputType;
  Measurer<LocalTime> measurer;
  static int TIME_BOUND = 10_000;

  /**
   * Initialises a pattern processor with the most basic information.
   *
   * @param value      The Measurement-Event to be evaluated
   * @param outputType The representation of the complex event created by the detection of pattern
   * @param measurer   Measurer used for calculating the output rates of the operator
   */
  public PatternProcessor(Measurement value, String outputType, Measurer<LocalTime> measurer) {
    this.value = value;
    this.outputType = outputType;
    this.measurer = measurer;
  }

  /**
   * Processes the given event and returns a non-empty result if the detection was successful
   *
   * @return A complex {@link Measurement} event if successful, else an empty {@link Optional}
   * @throws Exception Flink's exceptions
   */
  public abstract Optional<Measurement> process() throws Exception;

  /**
   * Searches in a {@link org.apache.flink.api.common.state.ListState} for the first element
   * that happened at most 10 seconds prior to the current {@link Measurement} event
   * being analysed.
   *
   * @param list      ListState containing the previously detected events
   * @param eventTime The timestamp of the current event
   * @return The index of the first valid event by success or the size of the list by a
   * failure
   */
  public static int findIndex(List<Measurement> list, long eventTime) {
    int index = 0;
    int size = list.size();
    while (index < size && eventTime - (list.get(index)).eventTime > TIME_BOUND) {
      index++;
    }
    return index;
  }
}
