package org.uni.potsdam.p1.actors;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

public class SinkOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {
  Map<String, Integer> eventIndexer;
  OutputTag<Metrics> outputTagProcessingTimes;
  OutputTag<Metrics> sosOutput;
  final int batchSize;
  double batch = 1.;
  String groupName;
  final Deque<LocalTime> timeQueue;
  long[] countArray;
  long[] eventStoreArray;
  int accessIndex = 0;
  double lastAverage = 0.;
  Metrics result;

  ListState<Measurement> list1;
  ListState<Measurement> list2;
  final Deque<Long> ptimeQueue;
  double lastAverage2 = 0.;
  double batch2 = 1.;
  String outKey;

  // variables for calculating the running average of the processing rates
  final Deque<LocalTime> processingRatesQueue;
  long[] countArrayProcessingRates;
  long[] eventStoreArrayProcessingRates;
  int accessIndexProcessingRates = 0;
  double lastAverageProcessingRates = 0.;
  HashMap<String, Integer> eventIndexer3;
  Metrics result2;
  Metrics result3;

  // set shedding shares for this operator
  Metrics sheddingShares;
  private boolean isShedding;

  public SinkOperator(String groupName, String[] inputKeys, String[] outputKeys, OutputTag<Metrics> outputProcessingRates, OutputTag<Metrics> sosOutput, int batchsize) {
    int size = outputKeys.length;
    countArray = new long[size];
    eventIndexer = new HashMap<>(size);
    result = new Metrics(groupName, "lambdaOut", size + 2);
    for (int i = 0; i < size; i++) {
      eventIndexer.put(outputKeys[i], i);
    }

    outputTagProcessingTimes = outputProcessingRates;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchSize = batchsize;
    timeQueue = new ArrayDeque<>(batchsize);
    eventStoreArray = new long[batchsize];
    ptimeQueue = new ArrayDeque<>(batchsize);
    processingRatesQueue = new ArrayDeque<>(batchsize);
    countArrayProcessingRates = new long[batchsize];
    eventStoreArrayProcessingRates = new long[batchsize];
    eventIndexer3 = new HashMap<>(inputKeys.length + 2);
    sheddingShares = new Metrics(groupName, "shares", inputKeys.length);
    outKey = outputKeys[0];
    for (int i = 0; i < inputKeys.length; i++) {
      eventIndexer3.put(inputKeys[i], i);
      sheddingShares.put(outKey + "_" + inputKeys[i], 0.);
    }
    result2 = new Metrics(groupName, "ptime", outputKeys.length + 2);
//    result2.put("shedding", Double.NEGATIVE_INFINITY);
    result3 = new Metrics(groupName, "mu", inputKeys.length + 2);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    list1 = getRuntimeContext().getListState(new ListStateDescriptor<>("list1", Measurement.class));
    list2 = getRuntimeContext().getListState(new ListStateDescriptor<>("list2", Measurement.class));
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int type = value.type;
    double prob = Math.random();
    boolean drop = isShedding && sheddingShares.get(outKey + "_" + value.type) > prob;
    LocalTime begin = LocalTime.now();
    if (!drop) {
      if ((type == 11 || type == 12)) {
        detectAND(value, out, list2, list1);
      } else {
        detectAND(value, out, list1, list2);
      }
    }
    LocalTime end = LocalTime.now();

    // collect processing times
    long timePattern = Duration.between(begin, end).toNanos();
    ptimeQueue.add(timePattern);

    // collect processing rates
    countArrayProcessingRates[eventIndexer3.get(String.valueOf(value.type))]++;
    eventStoreArrayProcessingRates[accessIndexProcessingRates] = value.type;
    accessIndexProcessingRates = (accessIndexProcessingRates + 1) % batchSize;
    processingRatesQueue.add(LocalTime.now());

    if (ptimeQueue.size() == batchSize) {

      // calculate average processing times per pattern
      long sum = 0L;
      for (long processingTime : ptimeQueue) {
        sum += processingTime;
      }
      double averageProcessingTime = sum / (batchSize * 1E9);
      result2.put("total", averageProcessingTime);
      result2.put(outKey, averageProcessingTime);
      result2.put("batch", batch2);
      lastAverage2 = averageProcessingTime;
      ptimeQueue.poll();

      // calculate average processing rates in the operator
      LocalTime oldestTimestamp = processingRatesQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, processingRatesQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result3.put("total", averageRate);
      for (String key : eventIndexer3.keySet()) {
        result3.put(key, (double) countArrayProcessingRates[eventIndexer3.get(key)] / elapsedTime);
      }
      result3.put("batch", batch2);
      ctx.output(outputTagProcessingTimes, result2);
      countArrayProcessingRates[eventIndexer3.get(String.valueOf(eventStoreArrayProcessingRates[accessIndexProcessingRates]))]--;
      lastAverageProcessingRates = averageRate;
      batch2++;
    }

    if (timeQueue.size() == batchSize) {
      LocalTime oldestTimestamp = timeQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, timeQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result.put("total", averageRate);
      result.put("batch", batch++);
      lastAverage = averageRate;
    }
  }

  private void detectAND(Measurement value, Collector<Measurement> out, ListState<Measurement> list2, ListState<Measurement> list1) throws Exception {
    List<Measurement> listo = (List<Measurement>) list2.get();
    if (!listo.isEmpty()) {
      Measurement current = listo.get(0);
      out.collect(new Measurement(Integer.parseInt(outKey), String.format("%s%n%s%n", value, current), 1));
      timeQueue.add(LocalTime.now());
      countArray[0]++;
      eventStoreArray[accessIndex] = Long.parseLong(outKey);
      accessIndex = (accessIndex + 1) % batchSize;
      list2.update(listo.subList(1, listo.size()));
    } else {
      list1.add(value);
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    String message = value.substring(0, index);
    if (message.equals("snap")) {
      long id = Long.parseLong(value.substring(index + 1));
      result.id = id;
      result2.id = id;
      result3.id = id;
      ctx.output(sosOutput, batch < 2 ? calculateCumulativeRateAverages(timeQueue, result) : result);
      ctx.output(sosOutput, batch2 < 2 ? calculateCumulativeTimeAverages(ptimeQueue, result2, batchSize) : result2);
      ctx.output(sosOutput, batch2 < 2 ? calculateCumulativeRateAverages(processingRatesQueue, result3) : result3);
      //TODO send metrics together
    } else if (message.equals(groupName)) {
      boolean sharesAreAllZero = true;
      for (String share : value.substring(index + 1).split(":")) {
        int separationIndex = share.indexOf("|");
        String currentShare = share.substring(separationIndex + 1);
        if (sharesAreAllZero && !currentShare.equals("0.0")) {
          sharesAreAllZero = false;
        }
        sheddingShares.put(share.substring(0, separationIndex), Double.valueOf(currentShare));
      }
      boolean informAnalyser = !sharesAreAllZero || isShedding;
      if (sharesAreAllZero) {
        isShedding = false;
        sheddingShares.put("shedding", Double.NEGATIVE_INFINITY);
      } else if (!isShedding) {
        isShedding = true;
        sheddingShares.put("shedding", Double.POSITIVE_INFINITY);
      }
      sheddingShares.put("batch", batch2);
      if (informAnalyser) {
        ctx.output(outputTagProcessingTimes, sheddingShares);
      }
    }
  }

  private Metrics calculateCumulativeRateAverages(Deque<LocalTime> collection, Metrics metrics) {
    LocalTime oldestTimestamp = collection.peek();
    double elapsedTime = (oldestTimestamp == null) ? 0 : (double) Duration.between(oldestTimestamp, collection.peekLast()).toMillis() / 1000;
    double averageRate = elapsedTime == 0 ? 0 : (double) collection.size() / elapsedTime;
    metrics.put("total", averageRate);
    return metrics;
  }

  private Metrics calculateCumulativeTimeAverages(Collection<Long> collection, Metrics metrics, long batchSize) {
    long sum = 0L;
    for (long processingTime : collection) {
      sum += processingTime;
    }
    double averageProcessingTime = sum / (batchSize * 1E9);
    metrics.put("total", averageProcessingTime);
    return metrics;
  }
}
