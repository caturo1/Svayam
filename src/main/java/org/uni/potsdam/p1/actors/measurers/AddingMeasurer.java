package org.uni.potsdam.p1.actors.measurers;

import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class is used to calculate the running average of the processing times of events
 * in each pattern of an operator. Every time an event is processed in an operator, an
 * instance of this class will calculate how much time it took in average for an event to
 * be processed at each individual pattern and in total. The running average is calculated
 * by summing the time value of the last batchSize events and dividing them by the size of
 * the queue (batchSize).
 */
public class AddingMeasurer extends Measurer<Long> {

  public Map<String, long[]> arrayConnector;

  /**
   * @see Measurer#Measurer(String, String[], String, int)
   */
  public AddingMeasurer(String operatorName, String[] eventTypes, String metricName, int batchSize) {
    super(operatorName, eventTypes, metricName, batchSize);
    arrayConnector = new HashMap<>(eventTypes.length);
    for (String eventType : eventTypes) {
      arrayConnector.put(eventType, new long[batchSize]);
    }
  }

  /**
   * Adds the total processing time taken to process an event at an operator in nanoseconds
   * to the running queue.
   *
   * @param begin Time when the event started being processed in the first pattern
   * @param end   Time when the event stopped being processed in the last pattern
   */
  public void updateQueue(LocalTime begin, LocalTime end) {
    accessIndex = (accessIndex + 1) % batchSize;
    runningQueue.add(Duration.between(begin, end).toNanos());
  }

  /**
   * Adds the processing time taken to process an event to the accumulated value of a pattern.
   *
   * @param pattern         The pattern in question
   * @param durationInNanos Time taken to process an event in the pattern
   */
  public void updatePatternTime(String pattern, long durationInNanos) {
    countArray[indexer.get(pattern)] += durationInNanos;
    arrayConnector.get(pattern)[accessIndex] = durationInNanos;
  }


  /**
   * Calculates and updates the newest averages of processing times
   *
   * @return The averages stored per type and in total gathered in a {@link Metrics} instance
   */
  @Override
  public Metrics getNewestAverages() {
    calculateNewestAverages(batchSize);
    for (String key : indexer.keySet()) {
      int index = indexer.get(key);
      countArray[index] -= arrayConnector.get(key)[accessIndex];
    }
    runningQueue.poll();
    return results;
  }

  /**
   * Calculates the newest averages of events per second for a queue of a specific size.
   *
   * @param queueSize Expected size of the queue
   */
  public void calculateNewestAverages(int queueSize) {
    boolean isEmpty = queueSize == 0;
    long sum = 0L;
    for (long processingTime : runningQueue) {
      sum += processingTime;
    }
    double averageProcessingTime = isEmpty ? 0 : sum / (queueSize * 1E9);
    results.put("total", averageProcessingTime);
    for (String key : indexer.keySet()) {
      int index = indexer.get(key);
      if (isEmpty) {
        results.put(key, (double) queueSize);
      } else {
        results.put(key, (double) countArray[index] / (queueSize * 1E9));
      }
    }
  }

  public double getNewestTime() {
    return Optional.ofNullable(runningQueue.peekLast()).orElse(0L) / 1E9;
  }

  @Override
  public Metrics getMetrics() {
    if (results.isEmpty()) {
      Optional<Long> oldestTimestamp = Optional.ofNullable(runningQueue.peek());
      oldestTimestamp.ifPresentOrElse(
        storedValue -> calculateNewestAverages(runningQueue.size()),
        () -> calculateNewestAverages(0)
      );
    }
    return results;
  }
}
