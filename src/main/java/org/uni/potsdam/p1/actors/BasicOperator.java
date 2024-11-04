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

public class BasicOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  String groupName;

  Map<String, Integer> eventIndexer;
  OutputTag<Metrics> outputRates;
  OutputTag<Metrics> sosOutput;
  final int batchSize;
  double batch = 1.;
  final Deque<LocalTime> timeQueue;
  long[] countArray;
  long[] eventStoreArray;
  int accessIndex = 0;
  double lastAverage = 0.;
  Metrics result;
  ListState<Measurement> zero;
  ListState<Measurement> first;
  ListState<Measurement> second;
  ListState<Measurement> third;
  String[] keys;
  OutputTag<Measurement> secondOutput;

  // variables for calculating the running average of the processing times
  OutputTag<Metrics> processingTimes;
  final Deque<Long> ptimeQueue;
  int accessIndex2;
  long[] countArray2;
  Map<String, Integer> eventIndexer2;
  double lastAverage2 = 0.;
  double batch2 = 1.;
  private final long[] rateStore1;
  private final long[] rateStore2;
  Metrics result2;

  // variables for calculating the running average of the processing rates
  final Deque<LocalTime> processingRatesQueue;
  long[] countArrayProcessingRates;
  long[] eventStoreArrayProcessingRates;
  int accessIndexProcessingRates = 0;
  double lastAverageProcessingRates = 0.;
  Map<String, Integer> eventIndexer3;
  Metrics result3;

  // set shedding shares for this operator
  Metrics sheddingShares;

  boolean isShedding = false;

  public BasicOperator(String groupName, String[] inputKeys, String[] outputKeys, OutputTag<Measurement> secondOutput, OutputTag<Metrics> processingTimes, OutputTag<Metrics> outputRates, OutputTag<Metrics> sosOutput, int batchsize) {

    this.keys = outputKeys;
    int size = keys.length;
    countArray = new long[size];
    eventIndexer = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      eventIndexer.put(keys[i], i);
    }
    result = new Metrics(groupName, "lambdaIn", size + 2);
    this.outputRates = outputRates;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchSize = batchsize;
    timeQueue = new ArrayDeque<>(batchsize);
    eventStoreArray = new long[batchsize];

    result.description = "lambdaOut";
    countArray2 = new long[size];
    eventIndexer2 = new HashMap<>(size);
    eventIndexer3 = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      eventIndexer2.put(keys[i], i);
    }
    sheddingShares = new Metrics(groupName, "shares", inputKeys.length * outputKeys.length + 1);
    for (int i = 0; i < inputKeys.length; i++) {
      eventIndexer3.put(inputKeys[i], i);
      for (String outkey : outputKeys) {
        sheddingShares.put(outkey + "_" + inputKeys[i], 0.);
      }
    }
    this.secondOutput = secondOutput;
    rateStore1 = new long[batchsize];
    rateStore2 = new long[batchsize];
    ptimeQueue = new ArrayDeque<>(batchsize);
    accessIndex2 = 0;
    this.processingTimes = processingTimes;
    processingRatesQueue = new ArrayDeque<>(batchsize);
    countArrayProcessingRates = new long[batchsize];
    eventStoreArrayProcessingRates = new long[batchsize];
    result2 = new Metrics(groupName, "ptime", outputKeys.length + 2);
    // TODO substitute with class and boolean field
    result3 = new Metrics(groupName, "mu", inputKeys.length + 2);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    zero = getRuntimeContext().getListState(new ListStateDescriptor<>("zero", Measurement.class));
    first = getRuntimeContext().getListState(new ListStateDescriptor<>("first", Measurement.class));
    second = getRuntimeContext().getListState(new ListStateDescriptor<>("second", Measurement.class));
    third = getRuntimeContext().getListState(new ListStateDescriptor<>("third", Measurement.class));
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    double prob = Math.random();
    boolean dropPattern1 = isShedding && sheddingShares.get(keys[0] + "_" + value.type) > prob;
    boolean dropPattern2 = isShedding && sheddingShares.get(keys[1] + "_" + value.type) > prob;
    LocalTime begin = LocalTime.now();
    if (!dropPattern1) {
      if (value.type == 0) {
        zero.add(value);
      } else if (value.type == 1) {
        List<Measurement> listOfZeros = (List<Measurement>) zero.get();
        int amountOfZeros = listOfZeros.size();
        if (amountOfZeros > 1) {
          int index = 0;
          Measurement firstCandidate = null;
          while (index < amountOfZeros && value.eventTime - (firstCandidate = listOfZeros.get(index)).eventTime > 10000) {
            index++;
          }
          if (index < amountOfZeros - 1) {
            Measurement secondCandidate = listOfZeros.get(index + 1);
            long diff = value.eventTime - secondCandidate.eventTime;
            if (diff > 0 && diff <= 10_000) {
              String key = keys[0];
              out.collect(new Measurement(Integer.parseInt(key), String.format("%s%n%s%n%s%n", value, secondCandidate, firstCandidate), 2));
              timeQueue.add(LocalTime.now());
              countArray[eventIndexer.get(key)]++;
              if (timeQueue.size() > batchSize) {
                countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
              }
              eventStoreArray[accessIndex] = Long.parseLong(key);
              accessIndex = (accessIndex + 1) % batchSize;
              zero.update(listOfZeros.subList(index + 2, amountOfZeros));
            } else {
              zero.update(listOfZeros.subList(index, amountOfZeros));
            }
          } else {
            zero.update(listOfZeros.subList(index, amountOfZeros));
          }
        }
      }
    }
    LocalTime middle = LocalTime.now();
    if (!dropPattern2) {
      if (value.type != 0) {
        List<Measurement> list1 = null;
        List<Measurement> list2 = null;
        ListState<Measurement> currentValueReference = null;
        ListState<Measurement> otherValueReference1 = null;
        ListState<Measurement> otherValueReference2 = null;
        if (value.type == 1) {
          list1 = (List<Measurement>) second.get();
          list2 = (List<Measurement>) third.get();
          currentValueReference = first;
          otherValueReference1 = second;
          otherValueReference2 = third;
        } else if (value.type == 2) {
          list1 = (List<Measurement>) first.get();
          list2 = (List<Measurement>) third.get();
          currentValueReference = second;
          otherValueReference1 = first;
          otherValueReference2 = third;
        } else if (value.type == 3) {
          list1 = (List<Measurement>) first.get();
          list2 = (List<Measurement>) second.get();
          currentValueReference = third;
          otherValueReference1 = first;
          otherValueReference2 = second;
        }
        if (list1 != null && list2 != null) {
          int index1 = 0;
          int size1 = list1.size();
          if (!list1.isEmpty()) {
            while (index1 < size1 && Math.abs(list1.get(index1).eventTime - value.eventTime) > 10_000) {
              index1++;
            }
          }
          int index2 = 0;
          int size2 = list2.size();
          if (!list2.isEmpty()) {
            while (index2 < size2 && Math.abs(list2.get(index2).eventTime - value.eventTime) > 10_000) {
              index2++;
            }
          }
          if (index1 < size1 && index2 < size2) {

            String key = keys[1];
            ctx.output(secondOutput, new Measurement(Integer.parseInt(key), String.format("%s%n%s%n%s%n", value, list1.get(index1), list2.get(index2)), 2));
            timeQueue.add(LocalTime.now());
            countArray[eventIndexer.get(key)]++;
            if (timeQueue.size() > batchSize) {
              countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
            }
            eventStoreArray[accessIndex] = Long.parseLong(key);
            accessIndex = (accessIndex + 1) % batchSize;
            otherValueReference1.update(list1.subList(index1 + 1, size1));
            otherValueReference2.update(list2.subList(index2 + 1, size2));
          } else {
            if (index1 > 0) {
              otherValueReference1.update(list1.subList(index1, size1));
            }
            if (index2 > 0) {
              otherValueReference2.update(list2.subList(index2, size2));
            }
            currentValueReference.add(value);
          }
        }
      }
    }

    // collect processing times
    LocalTime end = LocalTime.now();
    long timePattern1 = Duration.between(begin, middle).toNanos();
    long timePattern2 = Duration.between(middle, end).toNanos();
    countArray2[eventIndexer2.get(keys[0])] += timePattern1;
    countArray2[eventIndexer2.get(keys[1])] += timePattern2;
    rateStore1[accessIndex2] = timePattern1;
    rateStore2[accessIndex2] = timePattern2;
    accessIndex2 = (accessIndex2 + 1) % batchSize;
    ptimeQueue.add(Duration.between(begin, end).toNanos());

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
      for (String key : eventIndexer2.keySet()) {
        result2.put(key, (double) countArray2[eventIndexer2.get(key)] / (batchSize * 1E9));
      }
      result2.put("batch", batch2);
      countArray2[eventIndexer2.get(keys[0])] -= rateStore1[accessIndex2];
      countArray2[eventIndexer2.get(keys[1])] -= rateStore2[accessIndex2];
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
      result3.put("batch", batch2++);
      ctx.output(processingTimes, result2);
      countArrayProcessingRates[eventIndexer3.get(String.valueOf(eventStoreArrayProcessingRates[accessIndexProcessingRates]))]--;
      lastAverageProcessingRates = averageRate;
    }

    if (timeQueue.size() > batchSize) {
      for (int i = 0; i < timeQueue.size() - batchSize; i++) {
        timeQueue.poll();
      }
    }
    if (timeQueue.size() == batchSize) {
      LocalTime oldestTimestamp = timeQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, timeQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result.put("total", averageRate);
      for (String key : eventIndexer.keySet()) {
        result.put(key, (double) countArray[eventIndexer.get(key)] / elapsedTime);
      }
      result.put("batch", batch++);
      ctx.output(outputRates, result);
      countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
      lastAverage = averageRate;
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
      ctx.output(sosOutput, batch < 2 ? calculateCumulativeRateAverages(timeQueue, result, eventIndexer, countArray) : result);
      ctx.output(sosOutput, batch2 < 2 ? calculateCumulativeTimeAverages(ptimeQueue, result2, eventIndexer2, countArray2, batchSize) : result2);
      ctx.output(sosOutput, batch2 < 2 ? calculateCumulativeRateAverages(processingRatesQueue, result3, eventIndexer3, countArrayProcessingRates) : result3);
      //TODO send load shares and all metrics together at once
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
        ctx.output(processingTimes, sheddingShares);
      }
    }
  }

  private Metrics calculateCumulativeRateAverages(Deque<LocalTime> collection, Metrics metrics, Map<String, Integer> eventIndexer, long[] countArray) {
    LocalTime oldestTimestamp = collection.peek();
    double elapsedTime = (oldestTimestamp == null) ? 0 : (double) Duration.between(oldestTimestamp, collection.peekLast()).toMillis() / 1000;
    double averageRate = elapsedTime == 0 ? 0 : (double) collection.size() / elapsedTime;
    metrics.put("total", averageRate);
    for (String key : eventIndexer.keySet()) {
      metrics.put(key, elapsedTime > 0 ? ((double) countArray[eventIndexer.get(key)] / elapsedTime) : 0);
    }
    return metrics;
  }

  private Metrics calculateCumulativeTimeAverages(Collection<Long> collection, Metrics metrics, Map<String, Integer> eventIndexer, long[] countArray, long batchSize) {
    long sum = 0L;
    for (long processingTime : collection) {
      sum += processingTime;
    }
    double averageProcessingTime = sum / (batchSize * 1E9);
    metrics.put("total", averageProcessingTime);
    for (String key : eventIndexer.keySet()) {
      metrics.put(key, (double) countArray[eventIndexer.get(key)] / (batchSize * 1E9));
    }
    return metrics;
  }
}
