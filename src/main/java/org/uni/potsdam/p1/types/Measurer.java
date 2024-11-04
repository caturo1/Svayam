package org.uni.potsdam.p1.types;

import java.io.Serializable;
import java.util.*;

/**
 * This class represents the different kinds of rate measurers in the system. Its
 * subclasses are used to measure the output and processing rates as well as the
 * processing times of the operators. It stores a measured value of a non-specific type in
 * a fixed size Queue and calculates a running average of a specific metric on call.
 *
 * @param <T> The type of the measured value used to calculate the running average.
 */
public abstract class Measurer<T> implements Serializable {
  public Map<String, Integer> indexer;
  public int batchSize;
  public double batch = 1.;
  public Deque<T> runningQueue;
  public long[] countArray;
  public long[] storeArray;
  public int accessIndex = 0;
  public double lastAverage = 0.;
  public Metrics results;

  /**
   * Necessary empty constructor for Flink-serialization
   */
  public Measurer() {
  }

  Measurer(String operatorName, String[] inputTypes, String metricName, int batchSize) {
    this.batchSize = batchSize;
    runningQueue = new ArrayDeque<>(batchSize);
    storeArray = new long[batchSize];
    int size = inputTypes.length;
    countArray = new long[size];
    results = new Metrics(operatorName, metricName, size + 2);
    indexer = new HashMap(size);
    for (int i = 0; i < size; i++) {
      indexer.put(inputTypes[i], i);
    }
  }

  public abstract void update(Measurement value);

  /**
   * Proofs if the measurement queue of the operator is already full, so that the running
   * average can be updated.
   *
   * @return true if the queue reached the batchSize
   */
  public boolean isReady() {
    return runningQueue.size() == batchSize;
  }

  /**
   * Computes the newest running average for the specified {@link Metrics}, calculating
   * the total average for all events received as well as the average of the individual
   * event types.
   *
   * @return The updated {@link Metrics} instance containing the calculated running averages.
   */
  public abstract Metrics calculateNewestAverage();

  /**
   * Updates the {@link Metrics} instance managed by this object with the given id and
   * returns it.
   *
   * @return The updated {@link Metrics} instance.
   */
  public abstract Metrics getMetricsWithId(String id);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Measurer<?> measurer = (Measurer<?>) o;
    return batchSize == measurer.batchSize && Double.compare(batch, measurer.batch) == 0 && accessIndex == measurer.accessIndex && Double.compare(lastAverage, measurer.lastAverage) == 0 && Objects.equals(indexer, measurer.indexer) && Objects.equals(runningQueue, measurer.runningQueue) && Objects.deepEquals(countArray, measurer.countArray) && Objects.deepEquals(storeArray, measurer.storeArray) && Objects.equals(results, measurer.results);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexer, batchSize, batch, runningQueue, Arrays.hashCode(countArray), Arrays.hashCode(storeArray), accessIndex, lastAverage, results);
  }
}
