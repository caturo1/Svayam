package org.uni.potsdam.p1.actors.measurers;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.Objects;
import java.util.Optional;

/**
 * This class is used to calculate the running average of the quantity of different events
 * observed in a second. By each event it stores the current time of the system
 * in a measurement queue. Once the queue is full this object can calculate the
 * average amount of events seen per second by dividing the queue's size with the
 * time difference between the first and last measurements on the queue.
 * This class can be used to measure the output and processing rates of an operator.
 */
public class CountingMeasurer extends Measurer<LocalTime> {

  /**
   * @see Measurer#Measurer(String, String[], String, int)
   */
  public CountingMeasurer(String operatorName, String[] eventTypes, String metricName, int batchSize) {
    super(operatorName, eventTypes, metricName, batchSize);
  }

  /**
   * Increases the count of the given event type and add a time measurement to the queue
   *
   * @param eventType Type of the event currently being read by an operator.
   */
  @Override
  public void update(String eventType) {
    runningQueue.add(LocalTime.now());
    countArray[indexer.get(eventType)]++;
    if (runningQueue.size() > batchSize) {
      runningQueue.poll();
      countArray[indexer.get(String.valueOf(storeArray[accessIndex]))]--;
    }
    storeArray[accessIndex] = Long.parseLong(eventType);
    accessIndex = (accessIndex + 1) % batchSize;
  }

  /**
   * Calculates and updates the newest averages of events per second.
   *
   * @return The averages stored per type and in total gathered in a {@link Metrics} instance
   */
  @Override
  public Metrics getNewestAverages() {
    LocalTime oldestTimestamp = runningQueue.poll();
    LocalTime newestTimestamp = runningQueue.peekLast();
    calculateNewestAverages(oldestTimestamp, newestTimestamp, batchSize);
    countArray[indexer.get(String.valueOf(storeArray[accessIndex]))]--;
    batch++;
    return results;
  }

  public double getTotalAverageRate() {
    LocalTime oldestTimestamp = Objects.requireNonNull(runningQueue.peek());
    LocalTime newestTimestamp = runningQueue.peekLast();
    double elapsedTime = (double) Duration.between(oldestTimestamp, newestTimestamp).toMillis() / 1000;
    return elapsedTime == 0 ? elapsedTime : ((double) runningQueue.size() / elapsedTime);
  }

  @Override
  void calculateNewestAverages(int queueSize) {
    LocalTime oldestTimestamp = runningQueue.peek();
    LocalTime newestTimestamp = runningQueue.peekLast();
    calculateNewestAverages(oldestTimestamp, newestTimestamp, queueSize);
  }

  /**
   * Calculates the newest averages of events per second for a queue of a specific size.
   *
   * @param oldestTimestamp First timestamp in the queue
   * @param newestTimestamp Last timestamp in the queue
   * @param queueSize       Expected size of the queue
   */
  public void calculateNewestAverages(LocalTime oldestTimestamp, LocalTime newestTimestamp, int queueSize) {
    double elapsedTime = Duration.between(oldestTimestamp, newestTimestamp).toMillis() / 1000.;
    double averageRate = elapsedTime == 0 ? elapsedTime : ((double) queueSize / elapsedTime);
    results.put("total", averageRate);
    for (String key : indexer.keySet()) {
      results.put(key, elapsedTime == 0 ? 0 : ((double) countArray[indexer.get(key)] / elapsedTime));
    }
    results.put("batch", (double) batch);
  }

  /**
   * Returns the latest {@link Metrics} instance created by this object and adds an id
   * to it. This id can then be used in a keyed stream to group together
   * {@link Metrics} instances from different operators in a {@link KeyedStream}.
   * If the operator has not finished its first batch yet, then update the metrics with
   * empty values.
   *
   * @return The latest {@link Metrics} instance created by this measurer with the given id.
   */
  @Override
  public Metrics getMetricsWithId(String id) {
    if (batch < 2) {
      Optional<LocalTime> oldestTimestamp = Optional.ofNullable(runningQueue.peek());
      oldestTimestamp.ifPresentOrElse(
        storedValue -> calculateNewestAverages(storedValue, runningQueue.peekLast(), runningQueue.size()),
        () -> {
          LocalTime copycat = LocalTime.NOON;
          calculateNewestAverages(copycat, copycat, 0);
        }
      );
    }
    results.id = Long.parseLong(id);
    return results;
  }
}
