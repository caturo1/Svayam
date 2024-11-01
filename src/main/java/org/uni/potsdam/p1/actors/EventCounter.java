package org.uni.potsdam.p1.actors;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class EventCounter extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  Map<String, Integer> eventIndexer;
  OutputTag<Metrics> outputRates;
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

  public EventCounter(String groupName, String[] keys, OutputTag<Metrics> output, OutputTag<Metrics> sosOutput, int batchsize) {
    int size = keys.length;
    countArray = new long[size];
    eventIndexer = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      eventIndexer.put(keys[i], i);
    }
    result = new Metrics(groupName, "lambdaIn", size + 2);
    outputRates = output;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchSize = batchsize;
    timeQueue = new ArrayDeque<>(batchsize);
    eventStoreArray = new long[batchsize];
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    countArray[eventIndexer.get(String.valueOf(value.type))]++;
    eventStoreArray[accessIndex] = value.type;
    accessIndex = (accessIndex + 1) % batchSize;
    timeQueue.add(LocalTime.now());
    out.collect(value);
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
      result.id = Long.parseLong(value.substring(index + 1));
      ctx.output(sosOutput, result);
    }
  }
}
