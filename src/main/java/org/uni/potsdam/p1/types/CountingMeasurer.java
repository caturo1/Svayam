package org.uni.potsdam.p1.types;

import java.time.Duration;
import java.time.LocalTime;

/**
 * This class is used to calculate the running average of the quantity of different events
 * observed in a second. It can be used to measure the output and processing rates of
 * an operator.
 */
public class CountingMeasurer extends Measurer<LocalTime> {
  public CountingMeasurer(String operatorName, String[] inputTypes, String metricName, int batchSize) {
    super(operatorName, inputTypes, metricName, batchSize);
  }

  @Override
  public void update(Measurement value) {
    countArray[indexer.get(String.valueOf(value.type))]++;
    storeArray[accessIndex] = value.type;
    accessIndex = (accessIndex + 1) % batchSize;
    runningQueue.add(LocalTime.now());
  }

  @Override
  public Metrics calculateNewestAverage() {
    LocalTime oldestTimestamp = runningQueue.poll();
    double elapsedTime = (double) Duration.between(oldestTimestamp, runningQueue.peekLast()).toMillis() / 1000;
    double averageRate = (double) batchSize / elapsedTime;
    results.put("total", averageRate);
    for (String key : indexer.keySet()) {
      results.put(key, (double) countArray[indexer.get(key)] / elapsedTime);
    }
    results.put("batch", batch++);
    countArray[indexer.get(String.valueOf(storeArray[accessIndex]))]--;
    lastAverage = averageRate;
    return results;
  }

  @Override
  public Metrics getMetricsWithId(String id) {
    results.id = Long.parseLong(id);
    return results;
  }
}
