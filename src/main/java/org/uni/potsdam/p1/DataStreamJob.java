/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.uni.potsdam.p1;

import com.google.gson.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

class EventJsonDeserializer implements JsonDeserializer<Measurement>, Serializable {

  public EventJsonDeserializer() {
  }

  @Override
  public Measurement deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    JsonElement jsonTime = jsonObject.get("time");
    JsonElement jsonId = jsonObject.get("machine_id");
    return new Measurement(jsonId.getAsLong(), jsonTime.getAsLong());
  }
}

class Analyser extends KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, HashMap<String, Double>> {
  MapState<String, Double> map1;
  MapState<String, Double> map2;
  String[] lambdaKeys;
  String[] muKeys;
  String groupName;

  OutputTag<String> sosOutput;
  double lastAverage = 0.;

  public Analyser(String groupName, String[] lambdaKeys, String[] muKeys, OutputTag<String> sosOutput) {
    this.lambdaKeys = lambdaKeys;
    this.muKeys = muKeys;
    this.groupName = groupName;
    this.sosOutput = sosOutput;
  }

  @Override
  public void open(Configuration conf) {
    map1 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map1", String.class, Double.class));
    map2 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map2", String.class, Double.class));
  }

  @Override
  public void processElement1(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, HashMap<String, Double>>.Context ctx, Collector<HashMap<String, Double>> out) throws Exception {
    if (map2.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map1.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = value.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : muKeys) {
          double share = Objects.requireNonNullElse(map2.get("share" + key2 + "_" + key), 1.);
          weight += share * map2.get(key2);
        }
        calculatedP += (value.get(key) / (total == 0 ? 1 : total)) * weight;
      }
//      double oldValue = Objects.requireNonNullElse(lastAverage.value(), 0.);
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      if (lastAverage != 0.) {
        if (calculatedP / lastAverage >= 0.1) {
          value.put("B", B);
          out.collect(value);
          map2.put("mu", 1 / calculatedP);
//          out.collect(map2);
//          ctx.output(sosOutput, groupName);
          map2.clear();
        }
//        ctx.output(sosOutput, groupName + "B: " + B + "p: " + calculatedP);
      }
//      lastAverage.update(calculatedP);
      double ratio = calculatedP / (lastAverage == 0 ? 1 : lastAverage);
      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.get("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
      lastAverage = calculatedP;
    }
  }

  @Override
  public void processElement2(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, HashMap<String, Double>>.Context ctx, Collector<HashMap<String, Double>> out) throws Exception {
    if (map1.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map2.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = map1.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : muKeys) {
          double share = Objects.requireNonNullElse(value.get("share" + key2 + "_" + key), 1.);
          weight += share * value.get(key2);
        }
        calculatedP += (map1.get(key) / (total == 0 ? 1 : total)) * weight;
      }
//      double oldValue = Objects.requireNonNullElse(lastAverage.value(), 0.);
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      if (lastAverage != 0.) {
        if (calculatedP / lastAverage >= 0.1) {
          map1.put("B", B);
//          out.collect(map1);
          value.put("mu", 1 / calculatedP);
          out.collect(value);
//          ctx.output(sosOutput, groupName);
          map1.clear();
        }
//        ctx.output(sosOutput, groupName + "B: " + B + "p: " + calculatedP);
      }
//      lastAverage.update(calculatedP);
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.get("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
      lastAverage = calculatedP;
    }
  }
}

class ComplexOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  String[] outKeys;
  ListState<Measurement> list1;
  ListState<Measurement> list2;
  int cycle = 1;
  Map<String, Long> eventCounter = new HashMap<>(5);
  Map<String, Long> memory = new HashMap<>(5);
  HashMap<String, Double> outputRates = new HashMap<>(5);
  Map<String, Long> processingTime = new HashMap<>(5);
  Map<String, Long> processingMemory = new HashMap<>(5);
  HashMap<String, Double> processingRates = new HashMap<>(2);
  String groupName;

  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  OutputTag<HashMap<String, Double>> sosOutput;

  final int batchsize;
  boolean bound = false;
  double batch = 1;

  ComplexOperator(String groupName, String[] outKeys, OutputTag<HashMap<String, Double>> outputTagProcessingRates, OutputTag<HashMap<String, Double>> sosOutput, int batchsize) {
    this.outKeys = outKeys;
    for (String key : outKeys) {
      eventCounter.put(key, 0L);
      memory.put(key, 0L);
      outputRates.put(key, 0.);
      processingRates.put(key, 0.);
      processingTime.put(key, 0L);
      processingMemory.put(key, 0L);
    }
    this.outputTagProcessingRates = outputTagProcessingRates;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchsize = batchsize;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    list1 = getRuntimeContext().getListState(new ListStateDescriptor<>("list1", Measurement.class));
    list2 = getRuntimeContext().getListState(new ListStateDescriptor<>("list2", Measurement.class));
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!bound) {
      bound = true;
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    }
    long time = System.currentTimeMillis();
    int type = value.machineId;
    Integer key2 = Integer.valueOf(outKeys[0]);
    String outkey = key2.toString();
    String total = "total";
    boolean newEvent = false;
    if (type == 11 || type == 12) {
      List<Measurement> listo = (List<Measurement>) list2.get();
      if (!listo.isEmpty()) {
        Measurement current = listo.get(0);
        out.collect(new Measurement(key2, String.format("%s%n%s%n", value, current), 1));
        list2.update(listo.subList(1, listo.size()));
        newEvent = true;
      } else {
        list1.add(value);
      }
    } else {
      List<Measurement> listo = (List<Measurement>) list1.get();
      if (!listo.isEmpty()) {
        Measurement current = listo.get(0);
        out.collect(new Measurement(key2, String.format("%s%n%s%n", value, current), 1));
        list1.update(listo.subList(1, listo.size()));
        newEvent = true;
      } else {
        list2.add(value);
      }
    }
    if (newEvent) {
      eventCounter.put(outkey, eventCounter.get(outkey) + 1);
      eventCounter.put(total, eventCounter.get(total) + 1);
    }
//    long ptimeC = System.currentTimeMillis() - value.eventTime;
    long ptimeC = System.currentTimeMillis() - time;
    //TODO test time difference using the event times
    processingTime.put(outkey.toString(), processingTime.get(outkey.toString()) + ptimeC);
    long count = processingTime.get(total) + 1;
    processingTime.put(total, count);
    if (count % batchsize == 0) {
      processingRates.put("batch", batch);
      outputRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, processingRates);
      batch++;
//      processingMemory.clear();
//      processingTime.clear();
//      eventIndexer.clear();
//      memory.clear();
//      cycle = 1;
//      for (String elem : outKeys) {
//        processingMemory.put(elem, 0L);
//        processingTime.put(elem, 0L);
//        eventIndexer.put(elem, 0L);
//        memory.put(elem, 0L);
//      }
//      cycle = 1;
    }

  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!value.equals(groupName)) {
      ctx.output(sosOutput, processingRates);
      ctx.output(sosOutput, outputRates);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    for (String key : outKeys) {
      long oldTime = processingMemory.get(key);
      double newTime = (double) (oldTime + (processingTime.get(key) - oldTime)) / ((key.equals("total") ? 1 : 1000) * cycle);
      processingRates.put(key, newTime);
      processingMemory.put(key, processingTime.get(key));
      long oldValue = memory.get(key);
      double newValue = (double) (oldValue + (eventCounter.get(key) - oldValue)) / cycle;
      outputRates.put(key, newValue);
      memory.put(key, eventCounter.get(key));
    }
    cycle++;
  }
}

class Operator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {
  String[] outKeys;
  ListState<Measurement> zero;
  ListState<Measurement> first;
  ListState<Measurement> second;
  ListState<Measurement> third;
  int cycle = 1;
  Map<String, Long> processingTime = new HashMap<>(5);
  Map<String, Long> processingMemory = new HashMap<>(5);
  HashMap<String, Double> processingRates = new HashMap<>(2);
  String groupName;

  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  OutputTag<Measurement> secondOutput;
  OutputTag<HashMap<String, Double>> sosOutput;

  final int batchsize;
  boolean bound = false;
  double batch = 1;
  long count = 0;

  Operator(String groupName, String[] keys, OutputTag<HashMap<String, Double>> outputTagProcessingRates, OutputTag<Measurement> secondOutput, OutputTag<HashMap<String, Double>> sosOutput, int batchsize) {
    outKeys = keys;
    for (String key : outKeys) {
      processingRates.put(key, 0.);
      processingTime.put(key, 0L);
      processingMemory.put(key, 0L);
    }
    this.groupName = groupName;
    this.outputTagProcessingRates = outputTagProcessingRates;
    this.secondOutput = secondOutput;
    this.sosOutput = sosOutput;
    this.batchsize = batchsize;
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
    if (!bound) {
      bound = true;
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    }
    String outkey1 = outKeys[0];
    String outkey2 = outKeys[1];
    String total = "total";
    // check for pattern1
    long time = System.currentTimeMillis();
    if (value.machineId == 0) {
      zero.add(value);
    } else if (value.machineId == 1) {
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
            out.collect(new Measurement(Integer.valueOf(outkey1), String.format("%s%n%s%n%s%n", value, secondCandidate, firstCandidate), 2));
            zero.update(listOfZeros.subList(index + 2, amountOfZeros));
          } else {
            zero.update(listOfZeros.subList(index, amountOfZeros));
          }
        } else {
          zero.update(listOfZeros.subList(index, amountOfZeros));
        }
      }
    }
    long secondMeasurement = System.currentTimeMillis();
    long ptimeQ11 = System.currentTimeMillis() - time;
//    long ptimeQ11 = secondMeasurement - value.eventTime;

    //check for pattern2
    time = System.currentTimeMillis();
    if (value.machineId != 0) {

      List<Measurement> list1 = null;
      List<Measurement> list2 = null;
      ListState<Measurement> currentValueReference = null;
      ListState<Measurement> otherValueReference1 = null;
      ListState<Measurement> otherValueReference2 = null;
      if (value.machineId == 1) {
        list1 = (List<Measurement>) second.get();
        list2 = (List<Measurement>) third.get();
        currentValueReference = first;
        otherValueReference1 = second;
        otherValueReference2 = third;
      } else if (value.machineId == 2) {
        list1 = (List<Measurement>) first.get();
        list2 = (List<Measurement>) third.get();
        currentValueReference = second;
        otherValueReference1 = first;
        otherValueReference2 = third;
      } else if (value.machineId == 3) {
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

          ctx.output(secondOutput, new Measurement(Integer.valueOf(outkey2), String.format("%s%n%s%n%s%n", value, list1.get(index1), list2.get(index2)), 2));

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
    long ptimeQ12 = System.currentTimeMillis() - time;
//    long ptimeQ12 = System.currentTimeMillis() - (secondMeasurement - time) - value.eventTime;

    long timePattern1 = processingTime.get(outkey1) + ptimeQ11;
    long timePattern2 = processingTime.get(outkey2) + ptimeQ12;
    processingTime.put(outkey1, timePattern1);
    processingTime.put(outkey2, timePattern2);

//    long count = processingTime.getOrDefault(total, 0L) + 1;
    processingTime.put(total, processingTime.get(total) + timePattern1 + timePattern2);

    if (++count % batchsize == 0) {
      processingRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, processingRates);
      batch++;
//      processingMemory.clear();
//      processingTime.clear();
//      for (String elem : outKeys) {
//        processingMemory.put(elem, 0L);
//        processingTime.put(elem, 0L);
//      }
//      cycle = 1;
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!value.equals(groupName)) {
      ctx.output(sosOutput, processingRates);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    for (String key : outKeys) {
      long oldTime = processingMemory.get(key);
      double newTime = (double) (oldTime + (processingTime.get(key) - oldTime)) / ((key.equals("total") ? 1 : 1000) * cycle);
      processingRates.put(key, newTime);
      processingMemory.put(key, processingTime.get(key));
    }
    cycle++;
  }


}

class Counter extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  String[] keys;
  int cycle = 1;
  Map<String, Long> eventCounter = new HashMap<>(5);
  Map<String, Long> memory = new HashMap<>(5);
  HashMap<String, Double> outputRates = new HashMap<>(5);
  //  HashMap<String, Double> batchOutputRates = new HashMap<>(5);
//  HashMap<String, Double> memoryOutputRates = new HashMap<>(5);
  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  OutputTag<HashMap<String, Double>> sosOutput;
  final int batchsize;
  double batch = 1.;
  String groupName;
  final Queue<Instant> timeQueue;

  Counter(String groupName, String[] keys, OutputTag<HashMap<String, Double>> output, OutputTag<HashMap<String, Double>> sosOutput, int batchsize) {
    this.keys = keys;
    for (String key : keys) {
      eventCounter.put(key, 0L);
      memory.put(key, 0L);
      outputRates.put(key, 0.);
//      batchOutputRates.put(key, 0.);
//      memoryOutputRates.put(key, 0.);
    }
    outputTagProcessingRates = output;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchsize = batchsize;
    timeQueue = new ArrayDeque<>(batchsize);
  }

  boolean bound = false;

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!bound) {
      bound = true;
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    }
    String key = String.valueOf(value.machineId);
    eventCounter.put(key, eventCounter.get(key) + 1);
    long count = eventCounter.get("total") + 1;
    eventCounter.put("total", count);
    out.collect(new Measurement(value));
    timeQueue.add(Instant.now());
    if (count % (batchsize) == 0) {
//      for (String elm : keys) {
//        double toAdd = outputRatesPattern1.get(elm);
//        batchOutputRates.put(elm, batch > 1 ? batchOutputRates.get(elm) + ((toAdd - memoryOutputRates.get(elm)) / EVENT_BATCH) : (batchOutputRates.get(elm) + toAdd) / EVENT_BATCH);
//      }
      outputRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, outputRates);
      batch++;
//      for (String elem : keys) {
//        memory.put(elem, 0L);
//        eventIndexer.put(elem, 0L);
//      }
//      cycle = 1;
    }
//    else {
//      for (String elm : keys) {
//        double toStore = outputRatesPattern1.get(elm) + batchOutputRates.get(elm);
//        batchOutputRates.put(elm, toStore);
//        if (count == 1) {
//          memoryOutputRates.put(key, toStore);
//        }
//      }
//    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!value.equals(groupName)) {
      ctx.output(sosOutput, outputRates);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    for (String key : keys) {
      long oldValue = memory.get(key);
      long current = eventCounter.get(key);
      long newValue = current - oldValue;
      outputRates.put(key, ((double) oldValue + newValue) / cycle);
      memory.put(key, current);
    }
    cycle++;
  }
}

class SmartAnalyser extends KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics> {

  MapState<String, Double> map1;
  MapState<String, Double> map2;
  String[] lambdaKeys;
  String[] muKeys;
  String groupName;
  double LATENCY_BOUND = 0.001;
  double lastLambda = 0.;
  double lastMu = 0.;
  double lastPtime = 0.;

  OutputTag<String> sosOutput;
  double lastAverage = 0.;

  public SmartAnalyser(String groupName, String[] lambdaKeys, String[] muKeys, OutputTag<String> sosOutput) {
    this.lambdaKeys = lambdaKeys;
    this.muKeys = muKeys;
    this.groupName = groupName;
    this.sosOutput = sosOutput;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    map1 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map1", String.class, Double.class));
    map2 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map2", String.class, Double.class));
  }

  @Override
  public void processElement1(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if (map2.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map1.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = value.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : muKeys) {
          double share = Objects.requireNonNullElse(map2.get("share" + key2 + "_" + key), 1.);
          weight += share * map2.get(key2);
        }
        calculatedP += (value.get(key) / (total == 0 ? 1 : total)) * weight;
      }
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      double ratioLambda = Math.abs(1 - value.get("total") / (lastLambda == 0 ? 1 : lastLambda));
      double ratioPtime = Math.abs(1 - map2.get("total") / (lastPtime == 0 ? 1 : lastPtime));
      if (lastAverage != 0.) {
        if ((ratio > 0.1 || ratioLambda > 0.05 || ratioPtime > 0.05) && B > LATENCY_BOUND) {
          value.put("B", B);
          out.collect(value);
          map2.put("mu", 1 / calculatedP);
          Metrics output = new Metrics(value.name, "mu", muKeys.length + 2);
          for (Map.Entry<String, Double> entry : map2.entries()) {
            output.put(entry.getKey(), entry.getValue());
          }
          out.collect(output);
          out.collect(value);
          ctx.output(sosOutput, groupName);
          map2.clear();
        }
      }
//      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.get("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
      lastAverage = calculatedP;
      lastPtime = map2.get("total");
      lastLambda = total;
    }
  }

  @Override
  public void processElement2(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if (map1.isEmpty()) {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        map2.put(entry.getKey(), entry.getValue());
      }
    } else {
      double total = map1.get("total");
      double calculatedP = 0.;
      for (String key : lambdaKeys) {
        double weight = 0;
        for (String key2 : muKeys) {
          double share = Objects.requireNonNullElse(value.get("share" + key2 + "_" + key), 1.);
          weight += share * value.get(key2);
        }
        calculatedP += (map1.get(key) / (total == 0 ? 1 : total)) * weight;
      }
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      double ratio = Math.abs(1 - calculatedP / (lastAverage == 0 ? 1 : lastAverage));
      double ratioLambda = Math.abs(1 - map1.get("total") / (lastLambda == 0 ? 1 : lastLambda));
      double ratioPtime = Math.abs(1 - value.get("total") / (lastPtime == 0 ? 1 : lastPtime));
      if (lastAverage != 0.) {
        if ((ratio > 0.1 || ratioLambda > 0.05 || ratioPtime > 0.05) && B > LATENCY_BOUND) {
          map1.put("B", B);
          value.put("mu", 1 / calculatedP);
          Metrics output = new Metrics(value.name, "lambda", lambdaKeys.length + 2);
          for (Map.Entry<String, Double> entry : map1.entries()) {
            output.put(entry.getKey(), entry.getValue());
          }
          out.collect(output);
          out.collect(value);
          ctx.output(sosOutput, groupName + ":" + System.nanoTime());
          map1.clear();
        }
      }
//      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio + " batch: " + value.get("batch") + " map: " + total + " mu: " + (calculatedP == 0 ? 0 : 1 / calculatedP) + " last: " + lastAverage);
      lastAverage = calculatedP;
      lastPtime = value.get("total");
      lastLambda = total;
    }
  }
}

class Pattern implements Serializable {
  public String name;
  public ToDoubleFunction<HashMap<String, Double>> selFunction;

  public Pattern() {
  }

  public Pattern(String name, ToDoubleFunction<HashMap<String, Double>> selFunction) {
    this.name = name;
    this.selFunction = selFunction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pattern pattern = (Pattern) o;
    return Objects.equals(name, pattern.name) && Objects.equals(selFunction, pattern.selFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, selFunction);
  }
}

class Op implements Serializable {
  public String[] inputTypes;
  //  public Pattern[] patterns;
  public boolean isOverloaded = false;
  public String name;
  public HashMap<String, Integer> indexer = new HashMap<>(4);
  public Metrics[] metrics = new Metrics[4];

  public Op() {
  }

  public Op(String name, String[] inputTypes) { //} Pattern... patterns) {
    this.inputTypes = inputTypes;
    String[] metrics = new String[]{"lambdaIn", "lambdaOut", "mu", "ptime"};
    for (int i = 0; i < metrics.length; i++) {
      indexer.put(metrics[i], i);
    }
//    this.patterns = patterns;
    this.name = name;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (String index : indexer.keySet()) {
      String toAppend = String.format("%-10s\t%s\n", index+":", metrics[indexer.get(index)]);
      result.append(toAppend);
    }
    return result.toString();
  }

  public Metrics get(String key) {
    return metrics[indexer.get(key)];
  }

  public Metrics put(String key, Metrics value) {
    return metrics[indexer.get(key)] = value;
  }

  public boolean isReady() {
    for (Metrics metric : metrics) {
      if (metric == null) {
        return false;
      }
    }
    return true;
  }

  public void clear() {
    for (int i = 0; i < metrics.length; i++) {
      metrics[i] = null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Op op = (Op) o;
    return isOverloaded == op.isOverloaded && Objects.deepEquals(inputTypes, op.inputTypes) && Objects.equals(name, op.name) && Objects.equals(indexer, op.indexer) && Objects.deepEquals(metrics, op.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(inputTypes), isOverloaded, name, indexer, Arrays.hashCode(metrics));
  }
}


class Coordinator extends KeyedProcessFunction<Long, Metrics, String> {

  public HashMap<String, Integer> indexer;
  public Op[] operatorsList;
  public Metrics lambda;

  public Coordinator() {
  }

  public Coordinator(Op... operators) {
    indexer = new HashMap<>(operators.length);
    operatorsList = new Op[operators.length];
    for (int i = 0; i < operators.length; i++) {
      Op current = operators[i];
      indexer.put(current.name, i);
      operatorsList[i] = current;
    }
  }

  @Override
  public void processElement(Metrics value, KeyedProcessFunction<Long, Metrics, String>.Context ctx, Collector<String> out) throws Exception {
//    out.collect("Got: " + value);
    operatorsList[indexer.get(value.name)].put(value.description, value);
    if (value.name.matches("(o1|o2)") && value.description.equals("lambdaOut")) {
      if (lambda == null) {
        lambda = value;
      } else {
//        out.collect("lambada is:" + lambda);
        Metrics op1 = value.name.equals("o1") ? value : lambda;
        Metrics op2 = value == op1 ? lambda : value;
        Metrics op3 = new Metrics("o3", "lambdaIn", 3);
        Metrics op4 = new Metrics("o4", "lambdaIn", 3);
        Double lambda11 = op1.get("11");
        Double lambda12 = op1.get("12");
        Double lambda21 = op2.get("21");
        Double lambda22 = op2.get("22");
        op3.put("11", lambda11);
        op3.put("21", lambda21);
        op3.put("total", lambda21 + lambda11);
        op4.put("12", lambda12);
        op4.put("22", lambda22);
        op4.put("total", lambda22 + lambda12);
        operatorsList[indexer.get("o3")].put("lambdaIn", op3);
        operatorsList[indexer.get("o4")].put("lambdaIn", op4);
        lambda = null;
        out.collect("Got: " + op3);
        out.collect("Got: " + op4);
      }
    }
    boolean isReady = true;
    for (Op operator : operatorsList) {
      isReady &= operator.isReady();
    }
    if (isReady) {
      out.collect("Ready with:\n" + this.toString());
      clear();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Coordinator that = (Coordinator) o;
    return Objects.equals(indexer, that.indexer) && Objects.deepEquals(operatorsList, that.operatorsList);
  }

  public void clear() {
    for (Op operator : operatorsList) {
      operator.clear();
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexer, Arrays.hashCode(operatorsList));
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (String index : indexer.keySet()) {
      result.append(index).append("\n");
      result.append(operatorsList[indexer.get(index)]).append("\n");
    }
    return result.toString();
  }
}

class SmartJoiner extends KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics> {
  MapState<String, Double> mapState;
  String[] keys;
  String[] keys2;
  OutputTag<Metrics> secondOutput;

  public SmartJoiner(String[] keys, String[] keys2, OutputTag<Metrics> secondOutput) {
    this.keys = keys;
    this.keys2 = keys2;
    this.secondOutput = secondOutput;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapstate", String.class, Double.class));
  }

  @Override
  public void processElement1(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    processJob(mapState, value, keys, out, secondOutput, ctx);
  }

  @Override
  public void processElement2(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    processJob(mapState, value, keys2, out, secondOutput, ctx);
  }

  public static void processJob(MapState<String, Double> mapState, Metrics value, String[] keys, Collector<Metrics> out, OutputTag<Metrics> secondOutput, Context ctx) throws Exception {
    if (!mapState.isEmpty()) {
      Metrics result = new Metrics("o3", "lambda", 4);
      Metrics result2 = new Metrics("o4", "lambda", 4);
      fillMap(result, mapState, value, new String[]{keys[0], keys[1]});
      fillMap(result2, mapState, value, new String[]{keys[2], keys[3]});
      out.collect(result);
      ctx.output(secondOutput, result2);
      mapState.clear();
    } else {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        mapState.put(entry.getKey(), entry.getValue());
      }
    }
  }

  private static void fillMap(Metrics result, MapState<String, Double> mapState, Metrics value, String[] keys1) throws Exception {
    double first = value.get(keys1[0]);
    double second = mapState.get(keys1[1]);
    result.put("batch", value.get("batch"));
    result.put(keys1[0], first);
    result.put(keys1[1], second);
    result.put("total", first + second);
  }
}

class SmartComplexOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {
  Map<String, Integer> eventIndexer;
  OutputTag<Metrics> outputTagProcessingRates;
  OutputTag<Metrics> sosOutput;
  final int batchSize;
  double batch = 1.;
  String groupName;
  final Deque<Instant> timeQueue;
  long[] countArray;
  long[] eventStoreArray;
  int accessIndex = 0;
  double lastAverage = 0.;
  Metrics result;

  ListState<Measurement> list1;
  ListState<Measurement> list2;
  final Deque<Long> ptimeQueue;
  double lastAverage2 = 0.;
  double batch2 = 0.;
  String outKey;

  // variables for calculating the running average of the processing rates
  final Deque<Instant> processingRatesQueue;
  long[] countArrayProcessingRates;
  long[] eventStoreArrayProcessingRates;
  int accessIndexProcessingRates = 0;
  double lastAverageProcessingRates = 0.;
  double batch3 = 0.;
  HashMap<String, Integer> eventIndexer3;
  Metrics result2;
  Metrics result3;

  SmartComplexOperator(String groupName, String[] inputKeys, String[] outputKeys, OutputTag<Metrics> outputProcessingRates, OutputTag<Metrics> sosOutput, int batchsize) {
    int size = outputKeys.length;
    countArray = new long[size];
    eventIndexer = new HashMap<>(size);
    result = new Metrics(groupName, "lambdaOut", size + 2);
    for (int i = 0; i < size; i++) {
      eventIndexer.put(outputKeys[i], i);
    }

    outputTagProcessingRates = outputProcessingRates;
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
    for (int i = 0; i < inputKeys.length; i++) {
      eventIndexer3.put(inputKeys[i], i);
    }
    outKey = outputKeys[0];
    result2 = new Metrics(groupName, "ptime", outputKeys.length + 2);
    result3 = new Metrics(groupName, "mu", inputKeys.length + 2);
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    list1 = getRuntimeContext().getListState(new ListStateDescriptor<>("list1", Measurement.class));
    list2 = getRuntimeContext().getListState(new ListStateDescriptor<>("list2", Measurement.class));
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    Instant begin = Instant.now();
    int type = value.machineId;
    if (type == 11 || type == 12) {
      List<Measurement> listo = (List<Measurement>) list2.get();
      if (!listo.isEmpty()) {
        Measurement current = listo.get(0);
        out.collect(new Measurement(Integer.valueOf(outKey), String.format("%s%n%s%n", value, current), 1));
        timeQueue.add(Instant.now());
        countArray[0]++;
        eventStoreArray[accessIndex] = Long.valueOf(outKey);
        accessIndex = (accessIndex + 1) % batchSize;
        list2.update(listo.subList(1, listo.size()));
      } else {
        list1.add(value);
      }
    } else {
      List<Measurement> listo = (List<Measurement>) list1.get();
      if (!listo.isEmpty()) {
        Measurement current = listo.get(0);
        out.collect(new Measurement(Integer.valueOf(outKey), String.format("%s%n%s%n", value, current), 1));
        timeQueue.add(Instant.now());
        countArray[0]++;
        eventStoreArray[accessIndex] = Long.valueOf(outKey);
        accessIndex = (accessIndex + 1) % batchSize;
        list1.update(listo.subList(1, listo.size()));
      } else {
        list2.add(value);
      }
    }
    Instant end = Instant.now();

    // collect processing times
    long timePattern = Duration.between(begin, end).toNanos();
    ptimeQueue.add(timePattern);

    // collect processing rates
    countArrayProcessingRates[eventIndexer3.get(String.valueOf(value.machineId))]++;
    eventStoreArrayProcessingRates[accessIndexProcessingRates] = value.machineId;
    accessIndexProcessingRates = (accessIndexProcessingRates + 1) % batchSize;
    processingRatesQueue.add(Instant.now());

    if (ptimeQueue.size() == batchSize) {

      // calculate average processing times per pattern
      long sum = 0L;
      for (long processingTime : ptimeQueue) {
        sum += processingTime;
      }
      double averageProcessingTime = sum / (batchSize * 1E9);
      result2.put("total", averageProcessingTime);
      result2.put(outKey, averageProcessingTime);
      result2.put("batch", batch2++);
      ctx.output(outputTagProcessingRates, result2);
      lastAverage2 = averageProcessingTime;
      ptimeQueue.poll();

      // calculate average processing rates in the operator
      Instant oldestTimestamp = processingRatesQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, processingRatesQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result3.put("total", averageRate);
      for (String key : eventIndexer3.keySet()) {
        result3.put(key, (double) countArrayProcessingRates[eventIndexer3.get(key)] / elapsedTime);
      }
      result3.put("batch", batch3++);
      countArrayProcessingRates[eventIndexer3.get(String.valueOf(eventStoreArrayProcessingRates[accessIndexProcessingRates]))]--;
      lastAverageProcessingRates = averageRate;
    }

    if (timeQueue.size() == batchSize) {
      Instant oldestTimestamp = timeQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, timeQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result.put("total", averageRate);
      result.put("batch", batch++);
      lastAverage = averageRate;
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    if (!value.substring(0, index).equals(groupName)) {
      long id = Long.valueOf(value.substring(index + 1));
      result.id = id;
      result2.id = id;
      result3.id = id;
      ctx.output(sosOutput, result);
      ctx.output(sosOutput, result2);
      ctx.output(sosOutput, result3);
    }
  }
}

class SmartOperator extends SmartCounter {

  ListState<Measurement> zero;
  ListState<Measurement> first;
  ListState<Measurement> second;
  ListState<Measurement> third;
  String[] keys;
  OutputTag<Measurement> secondOutput;

  // variables for calculating the running average of the processing times
  OutputTag<Metrics> outputRatesPattern1;
  final Deque<Long> ptimeQueue;
  int accessIndex2;
  long[] countArray2;
  Map<String, Integer> eventIndexer2;
  double lastAverage2 = 0.;
  double batch2 = 0.;
  private final long[] rateStore1;
  private final long[] rateStore2;

  // variables for calculating the running average of the processing rates
  final Deque<Instant> processingRatesQueue;
  long[] countArrayProcessingRates;
  long[] eventStoreArrayProcessingRates;
  int accessIndexProcessingRates = 0;
  double lastAverageProcessingRates = 0.;
  double batch3 = 0.;
  Map<String, Integer> eventIndexer3;
  Metrics result2;
  Metrics result3;

  SmartOperator(String groupName, String[] inputKeys, String[] outputKeys, OutputTag<Measurement> secondOutput, OutputTag<Metrics> outputTagProcessingRates, OutputTag<Metrics> outputRates, OutputTag<Metrics> sosOutput, int batchsize) {
    super(groupName, outputKeys, outputTagProcessingRates, sosOutput, batchsize);
    result.description = "lambdaOut";
    this.keys = outputKeys;
    int size = outputKeys.length;
    countArray2 = new long[size];
    eventIndexer2 = new HashMap<>(size);
    eventIndexer3 = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      eventIndexer2.put(keys[i], i);
    }
    for (int i = 0; i < inputKeys.length; i++) {
      eventIndexer3.put(inputKeys[i], i);
    }
    this.secondOutput = secondOutput;
    rateStore1 = new long[batchsize];
    rateStore2 = new long[batchsize];
    ptimeQueue = new ArrayDeque<>(batchsize);
    accessIndex2 = 0;
    this.outputRatesPattern1 = outputRates;
    processingRatesQueue = new ArrayDeque<>(batchsize);
    countArrayProcessingRates = new long[batchsize];
    eventStoreArrayProcessingRates = new long[batchsize];
    result2 = new Metrics(groupName, "ptime", outputKeys.length + 2);
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
    Instant begin = Instant.now();
    if (value.machineId == 0) {
      zero.add(value);
    } else if (value.machineId == 1) {
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
            out.collect(new Measurement(Integer.valueOf(key), String.format("%s%n%s%n%s%n", value, secondCandidate, firstCandidate), 2));
            timeQueue.add(Instant.now());
            countArray[eventIndexer.get(key)]++;
            if (timeQueue.size() > batchSize) {
              countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
            }
            eventStoreArray[accessIndex] = Long.valueOf(key);
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
    Instant middle = Instant.now();
    if (value.machineId != 0) {

      List<Measurement> list1 = null;
      List<Measurement> list2 = null;
      ListState<Measurement> currentValueReference = null;
      ListState<Measurement> otherValueReference1 = null;
      ListState<Measurement> otherValueReference2 = null;
      if (value.machineId == 1) {
        list1 = (List<Measurement>) second.get();
        list2 = (List<Measurement>) third.get();
        currentValueReference = first;
        otherValueReference1 = second;
        otherValueReference2 = third;
      } else if (value.machineId == 2) {
        list1 = (List<Measurement>) first.get();
        list2 = (List<Measurement>) third.get();
        currentValueReference = second;
        otherValueReference1 = first;
        otherValueReference2 = third;
      } else if (value.machineId == 3) {
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
          ctx.output(secondOutput, new Measurement(Integer.valueOf(key), String.format("%s%n%s%n%s%n", value, list1.get(index1), list2.get(index2)), 2));
          timeQueue.add(Instant.now());
          countArray[eventIndexer.get(key)]++;
          if (timeQueue.size() > batchSize) {
            countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
          }
          eventStoreArray[accessIndex] = Long.valueOf(key);
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

    // collect processing times
    Instant end = Instant.now();
    long timePattern1 = Duration.between(begin, middle).toNanos();
    long timePattern2 = Duration.between(middle, end).toNanos();
    countArray2[eventIndexer2.get(keys[0])] += timePattern1;
    countArray2[eventIndexer2.get(keys[1])] += timePattern2;
    rateStore1[accessIndex2] = timePattern1;
    rateStore2[accessIndex2] = timePattern2;
    accessIndex2 = (accessIndex2 + 1) % batchSize;
    ptimeQueue.add(Duration.between(begin, end).toNanos());

    // collect processing rates
    countArrayProcessingRates[eventIndexer3.get(String.valueOf(value.machineId))]++;
    eventStoreArrayProcessingRates[accessIndexProcessingRates] = value.machineId;
    accessIndexProcessingRates = (accessIndexProcessingRates + 1) % batchSize;
    processingRatesQueue.add(Instant.now());

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
      result2.put("batch", batch2++);
      ctx.output(outputTagProcessingRates, result2);
      countArray2[eventIndexer2.get(keys[0])] -= rateStore1[accessIndex2];
      countArray2[eventIndexer2.get(keys[1])] -= rateStore2[accessIndex2];
      lastAverage2 = averageProcessingTime;
      ptimeQueue.poll();

      // calculate average processing rates in the operator
      Instant oldestTimestamp = processingRatesQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, processingRatesQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result3.put("total", averageRate);
      for (String key : eventIndexer3.keySet()) {
        result3.put(key, (double) countArrayProcessingRates[eventIndexer3.get(key)] / elapsedTime);
      }
      result3.put("batch", batch3++);
      countArrayProcessingRates[eventIndexer3.get(String.valueOf(eventStoreArrayProcessingRates[accessIndexProcessingRates]))]--;
      lastAverageProcessingRates = averageRate;
    }

    if (timeQueue.size() > batchSize) {
      for (int i = 0; i < timeQueue.size() - batchSize; i++) {
        timeQueue.poll();
      }
    }
    if (timeQueue.size() == batchSize) {
      Instant oldestTimestamp = timeQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, timeQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result.put("total", averageRate);
      for (String key : eventIndexer.keySet()) {
        result.put(key, (double) countArray[eventIndexer.get(key)] / elapsedTime);
      }
      result.put("batch", batch++);
      ctx.output(outputRatesPattern1, result);
      countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
      lastAverage = averageRate;
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    if (!value.substring(0, index).equals(groupName)) {
      long id = Long.valueOf(value.substring(index + 1));
      result.id = id;
      result2.id = id;
      result3.id = id;
      ctx.output(sosOutput, result);
      ctx.output(sosOutput, result2);
      ctx.output(sosOutput, result3);
    }
  }
}

class SmartCounter extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  Map<String, Integer> eventIndexer;
  OutputTag<Metrics> outputTagProcessingRates;
  OutputTag<Metrics> sosOutput;
  final int batchSize;
  double batch = 1.;
  String groupName;
  final Deque<Instant> timeQueue;
  long[] countArray;
  long[] eventStoreArray;
  int accessIndex = 0;
  double lastAverage = 0.;
  Metrics result;

  SmartCounter(String groupName, String[] keys, OutputTag<Metrics> output, OutputTag<Metrics> sosOutput, int batchsize) {
    int size = keys.length;
    countArray = new long[size];
    eventIndexer = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      eventIndexer.put(keys[i], i);
    }
    result = new Metrics(groupName, "lambdaIn", size + 2);
    outputTagProcessingRates = output;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
    this.batchSize = batchsize;
    timeQueue = new ArrayDeque<>(batchsize);
    eventStoreArray = new long[batchsize];
  }

  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    countArray[eventIndexer.get(String.valueOf(value.machineId))]++;
    eventStoreArray[accessIndex] = value.machineId;
    accessIndex = (accessIndex + 1) % batchSize;
    timeQueue.add(Instant.now());
    out.collect(value);
    if (timeQueue.size() == batchSize) {
      Instant oldestTimestamp = timeQueue.poll();
      double elapsedTime = (double) Duration.between(oldestTimestamp, timeQueue.peekLast()).toMillis() / 1000;
      double averageRate = (double) batchSize / elapsedTime;
      result.put("total", averageRate);
      for (String key : eventIndexer.keySet()) {
        result.put(key, (double) countArray[eventIndexer.get(key)] / elapsedTime);
      }
      result.put("batch", batch++);
      ctx.output(outputTagProcessingRates, result);
      countArray[eventIndexer.get(String.valueOf(eventStoreArray[accessIndex]))]--;
      lastAverage = averageRate;
    }
  }

  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    int index = value.indexOf(":");
    if (!value.substring(0, index).equals(groupName)) {
      long id = Long.valueOf(value.substring(index + 1));
      result.id = id;
      ctx.output(sosOutput, result);
    }
  }
}

class SourceCounter extends ProcessFunction<Measurement, Measurement> {
  String[] keys;
  int cycle = 1;
  HashMap<String, Long> eventCounter;
  Map<String, Long> memory = new HashMap<>(5);
  HashMap<String, Double> outputRates = new HashMap<>(5);
  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  final int EVENT_BATCH = 100;
  double batch = 1;
  double id;

  SourceCounter(OutputTag<HashMap<String, Double>> output, String[] keys, double id) {
    this.keys = keys;
    for (String key : keys) {
      eventCounter.put(key, 0L);
      memory.put(key, 0L);
      outputRates.put(key, 0.);
    }
    outputTagProcessingRates = output;
    this.id = id;
  }

  boolean bound = false;

  @Override
  public void processElement(Measurement value, ProcessFunction<Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    if (!bound) {
      bound = true;
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
    }
    String key = String.valueOf(value.machineId);
    eventCounter.put(key, eventCounter.get(key) + 1);
    long count = eventCounter.get("total") + 1;
    eventCounter.put("total", count);
    out.collect(new Measurement(value));
    if (count % EVENT_BATCH == 0) {
      outputRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, outputRates);
      batch++;
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + TimeUnit.SECONDS.toMillis(1));
//    String beggining = memory.toString();
//    HashMap<String, Double> increase = new HashMap<>(5);
    for (String key : keys) {
//      double oldRate = outputRatesPattern1.get(key);
      long oldValue = memory.get(key);
      double newValue = (double) (oldValue + (eventCounter.get(key) - oldValue)) / cycle;
      outputRates.put(key, newValue);
      memory.put(key, eventCounter.get(key));
//      double relation = (newValue / (oldRate == 0 ? 1 : oldRate));
//      increase.put(key, relation);
    }
    cycle++;
  }
}

public class DataStreamJob {

  // define the number of sinks and sources
  private static final int NUMBER_OF_SINKS = 2;
  private static final int NUMBER_OF_SOURCES = 2;

  private static final int CONTROL_BATCH_SIZE = 100;
  // define the number of entries and
  private static final int BATCH_SIZE = 10_000;
  private static final GeneratorFunction<Long, Measurement>
    MEASUREMENT_GENERATOR1 = index -> {
    return new Measurement((int) (Math.random() * 4), 1, index);
  };
  private static final GeneratorFunction<Long, Measurement>
    MEASUREMENT_GENERATOR2 = index -> {
    return new Measurement((int) (Math.random() * 4), 2, index);
  };

  private static final GeneratorFunction<Long, String>
    SOS_GENERATOR = index -> "sos";
  private final List<DataGeneratorSource<Measurement>> sources = new ArrayList<>();

  private static final String kafkaAddress = "kafka1:19092";

  Op[] operators = new Op[]{
    new Op("o1",
      new String[]{"1", "2", "3", "0"}),
//      new Pattern("Seq(001)", map -> Math.min(map.get("1").doubleValue() * (1 - map.get("x1")), map.get("0").doubleValue() * (1 - map.get("x0")) / 2)),
//      new Pattern("123",
//        map -> Math.min(map.get("1").doubleValue() * (1 - map.get("x1")),
//          Math.min(map.get("2").doubleValue() * (1 - map.get("x2")), map.get("3").doubleValue() * (1 - map.get("x3")))))),

    new Op("o2",
      new String[]{"1", "2", "3", "0"}),
//      new Pattern("Seq(001)", map -> Math.min(map.get("1").doubleValue() * (1 - map.get("x1")), map.get("0").doubleValue() * (1 - map.get("x0")) / 2)),
//      new Pattern("123",
//        map -> Math.min(map.get("1").doubleValue() * (1 - map.get("x1")),
//          Math.min(map.get("2").doubleValue() * (1 - map.get("x2")), map.get("3").doubleValue() * (1 - map.get("x3")))))),

    new Op("o3",
      new String[]{"11", "21"}),
//      new Pattern("Q11Q21", map -> Math.min(map.get("11").doubleValue() * (1 - map.get("x11")), map.get("21").doubleValue() * (1 - map.get("x21"))))),

    new Op("o4",
      new String[]{"12", "22"})
//      new Pattern("Q12Q22", map -> Math.min(map.get("12").doubleValue() * (1 - map.get("x12")), map.get("22").doubleValue() * (1 - map.get("x22")))))
  };

  KafkaSource<String> globalChannelIn = KafkaSource.<String>builder()
    .setBootstrapServers(kafkaAddress)
    .setTopics("global")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
  //
  KafkaSink<String> control = KafkaSink.<String>builder()
    .setBootstrapServers(kafkaAddress)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("globalOut")
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .build();

  KafkaSink<String> globalChannelOut = KafkaSink.<String>builder()
    .setBootstrapServers(kafkaAddress)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("global")
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .build();

  public DataStreamJob(int numberOfSources, int numberOfSinks) {
    for (int i = 0; i < numberOfSources; i++) {
      sources.add(new DataGeneratorSource<>(
        (i == 0 ? MEASUREMENT_GENERATOR1 : MEASUREMENT_GENERATOR2),
        BATCH_SIZE,
        RateLimiterStrategy.perSecond(100),
        TypeInformation.of(Measurement.class)));
    }

  }

  public static void main(String[] args) throws Exception {
    DataStreamJob job = new DataStreamJob(NUMBER_OF_SOURCES, NUMBER_OF_SINKS);
    job.execute();
  }

  public JobExecutionResult execute() throws Exception {

    // start execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // define Sources
    KeyedStream<String, Integer> global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global").keyBy(s1 -> 1);

    DataStream<Measurement> source1 = env.fromSource(
      sources.get(0),
      WatermarkStrategy.noWatermarks(),
      "Generator1").name("Source1");

    DataStream<Measurement> source2 = env.fromSource(
      sources.get(1),
      WatermarkStrategy.noWatermarks(),
      "Generator2").name("Source2");

    // Define side output channels for interprocess communication
    final OutputTag<Metrics> toCoordinator = new OutputTag<>("out_to_coordinator") {
    };
    final OutputTag<String> toKafka = new OutputTag<>("out_to_kafka") {
    };
    final OutputTag<Metrics> toAnalyser1 = new OutputTag<>("out_to_analyser1") {
    };
    final OutputTag<Metrics> outputOp1 = new OutputTag<>("out_of_op1") {
    };
    final OutputTag<Metrics> outputOp2 = new OutputTag<>("out_of_op2") {
    };
    final OutputTag<Metrics> toAnalyser2 = new OutputTag<>("out_to_analyser2") {
    };
    final OutputTag<Metrics> toAnalyser3 = new OutputTag<>("out_to_analyser3") {
    };
    final OutputTag<Metrics> toAnalyser4 = new OutputTag<>("out_to_analyser4") {
    };
    final OutputTag<Measurement> toOperator4 = new OutputTag<>("out_to_operator4") {
    };

    // count the output rates of the sources per second and forward the events to the operators
    SingleOutputStreamOperator<Measurement> counter1 = source1.keyBy(me -> 1).connect(global).process(new SmartCounter("o1", new String[]{"0", "1", "2", "3"}, toAnalyser1, toCoordinator, CONTROL_BATCH_SIZE));
    SingleOutputStreamOperator<Measurement> counter2 = source2.keyBy(me -> 1).connect(global).process(new SmartCounter("o2", new String[]{"0", "1", "2", "3"}, toAnalyser2, toCoordinator, CONTROL_BATCH_SIZE));

    // set operators 1 and 2 and collect both their processing (mu) and their output rates (lambda)
    SingleOutputStreamOperator<Measurement> operator1 = counter1.keyBy(me -> 1).connect(global).process(new SmartOperator("o1", new String[]{"0", "1", "2", "3"}, new String[]{"11", "12"}, toOperator4, toAnalyser1, outputOp1, toCoordinator, CONTROL_BATCH_SIZE));
    DataStream<Metrics> lambdaOperator1 = operator1.getSideOutput(outputOp1);

    SingleOutputStreamOperator<Measurement> operator2 = counter2.keyBy(me -> 1).connect(global).process(new SmartOperator("o2", new String[]{"0", "1", "2", "3"}, new String[]{"21", "22"}, toOperator4, toAnalyser2, outputOp2, toCoordinator, CONTROL_BATCH_SIZE));
    DataStream<Metrics> lambdaOperator2 = operator2.getSideOutput(outputOp2);

    // connect the stream of output rates of the sources with the stream of processing rates of the operators
    SingleOutputStreamOperator<Metrics> analyser1 = counter1.getSideOutput(toAnalyser1).keyBy(map -> map.get("batch"))
      .connect(operator1.getSideOutput(toAnalyser1).keyBy(map -> map.get("batch")))
      .process(new SmartAnalyser("o1", new String[]{"1", "2", "3", "0"}, new String[]{"11", "12"}, toKafka));

    SingleOutputStreamOperator<Metrics> analyser2 = counter2.getSideOutput(toAnalyser2).keyBy(map -> map.get("batch"))
      .connect(operator2.getSideOutput(toAnalyser2).keyBy(map -> map.get("batch")))
      .process(new SmartAnalyser("o2", new String[]{"1", "2", "3", "0"}, new String[]{"21", "22"}, toKafka));

    // set operators 3 and 4 and collect both their processing (mu) and their output rates (lambda)
    SingleOutputStreamOperator<Measurement> operator3 = operator1.union(operator2).keyBy(me -> 1).connect(global).process(new SmartComplexOperator("o3", new String[]{"11", "21"}, new String[]{"1000"}, toAnalyser3, toCoordinator, CONTROL_BATCH_SIZE));

    SingleOutputStreamOperator<Measurement> operator4 = operator1.getSideOutput(toOperator4).union(operator2.getSideOutput(toOperator4)).keyBy(me -> 1).connect(global).process(new SmartComplexOperator("o4", new String[]{"12", "22"}, new String[]{"2000"}, toAnalyser4, toCoordinator, CONTROL_BATCH_SIZE));


    SingleOutputStreamOperator<Metrics> joined = operator1.getSideOutput(outputOp1).keyBy(map -> map.get("batch")).connect(operator2.getSideOutput(outputOp2).keyBy(map -> map.get("batch"))).process(
      new SmartJoiner(new String[]{"11", "21", "12", "22"}, new String[]{"21", "11", "22", "12"}, toAnalyser4));

    SingleOutputStreamOperator<Metrics> analyser3 = joined.keyBy(map -> map.get("batch"))
      .connect(operator3.getSideOutput(toAnalyser3).keyBy(map -> map.get("batch")))
      .process(new SmartAnalyser("o3", new String[]{"11", "21"}, new String[]{"1000"}, toKafka));

    SingleOutputStreamOperator<Metrics> analyser4 = joined.getSideOutput(toAnalyser4).keyBy(map -> map.get("batch"))
      .connect(operator4.getSideOutput(toAnalyser4).keyBy(map -> map.get("batch")))
      .process(new SmartAnalyser("o4", new String[]{"12", "22"}, new String[]{"2000"}, toKafka));

    analyser1.getSideOutput(toKafka)
      .union(analyser2.getSideOutput(toKafka))
      .union(analyser3.getSideOutput(toKafka))
      .union(analyser4.getSideOutput(toKafka))
      .sinkTo(globalChannelOut);

    DataStream<Metrics> streamToCoordinator = analyser1.union(analyser2)
      .union(analyser3)
      .union(analyser4)
      .union(counter1.getSideOutput(toCoordinator))
      .union(counter2.getSideOutput(toCoordinator))
      .union(operator1.getSideOutput(toCoordinator))
      .union(operator2.getSideOutput(toCoordinator))
      .union(operator3.getSideOutput(toCoordinator))
      .union(operator4.getSideOutput(toCoordinator));

    streamToCoordinator
      .keyBy(metric -> metric.id).process(new Coordinator(operators))
      .sinkTo(control);

    // connect the stream of output rates of the sources with the stream of processing rates of the operators

//    // count the output rates of the sources per second and forward the events to the operators
//    SingleOutputStreamOperator<Measurement> counter1 = source1.keyBy(me -> 1).connect(global).process(new Counter("o1", new String[]{"0", "1", "2", "3", "total"}, toAnalyser1, toCoordinator, CONTROL_BATCH_SIZE));
//    SingleOutputStreamOperator<Measurement> counter2 = source2.keyBy(me -> 1).connect(global).process(new Counter("o2", new String[]{"0", "1", "2", "3", "total"}, toAnalyser2, toCoordinator, CONTROL_BATCH_SIZE));
//
//
//
//    SingleOutputStreamOperator<Measurement> operator1 = counter1.keyBy(me -> 1).connect(global).process(new Operator("o1", new String[]{"11", "12", "total"}, toAnalyser1, toOperator4, toCoordinator, CONTROL_BATCH_SIZE));
//
//    SingleOutputStreamOperator<Measurement> operator2 = counter2.keyBy(me -> 1).connect(global).process(new Operator("o2", new String[]{"21", "22", "total"}, toAnalyser2, toOperator4, toCoordinator, CONTROL_BATCH_SIZE));
//    DataStream<Measurement> operator2SecondOutput = operator2.getSideOutput(toOperator4);
//    DataStream<HashMap<String, Double>> muOperator2 = operator2.getSideOutput(toAnalyser2);
//
//    // connect the stream of output rates of the sources with the stream of processing rates of the operators
//    SingleOutputStreamOperator<HashMap<String, Double>> analyser1 = lambdaSource1.keyBy(map -> map.get("batch")).connect(muOperator1.keyBy(map -> map.get("batch")))
//      .process(new Analyser("o1", new String[]{"0", "1", "2", "3"}, new String[]{"11", "12"}, toKafka));
//
//    SingleOutputStreamOperator<HashMap<String, Double>> analyser2 = lambdaSource2.keyBy(map -> map.get("batch")).connect(muOperator2.keyBy(map -> map.get("batch")))
//      .process(new Analyser("o2", new String[]{"0", "1", "2", "3"}, new String[]{"21", "22"}, toKafka));
//
//    // count the input rates for operators 3 and 4
//    SingleOutputStreamOperator<Measurement> counter3 = operator1.union(operator2).keyBy(me -> 1).connect(global).process(new Counter("o3", new String[]{"11", "21", "total"}, toAnalyser3, toCoordinator, CONTROL_BATCH_SIZE));
//    SingleOutputStreamOperator<Measurement> counter4 = operator1SecondOutput.union(operator2SecondOutput).keyBy(me -> 1).connect(global).process(new Counter("o4", new String[]{"12", "22", "total"}, toAnalyser4, toCoordinator, CONTROL_BATCH_SIZE));
//
//    // set operators 3 and 4 and collect both their processing (mu) and their output rates (lambda)
//    SingleOutputStreamOperator<Measurement> operator3 = counter3.keyBy(me -> 1).connect(global).process(
//      new ComplexOperator("o3", new String[]{"1000", "total"}, toAnalyser3, toCoordinator, CONTROL_BATCH_SIZE));
//    DataStream<HashMap<String, Double>> muOperator3 = operator3.getSideOutput(toAnalyser3);
//
//    SingleOutputStreamOperator<Measurement> operator4 = counter4.keyBy(me -> 1).connect(global).process(
//      new ComplexOperator("o4", new String[]{"2000", "total"}, toAnalyser4, toCoordinator, CONTROL_BATCH_SIZE));
//    DataStream<HashMap<String, Double>> muOperator4 = operator4.getSideOutput(toAnalyser4);
//
//
////    analyser1.union(analyser2).union(analyser3).union(analyser4).map(HashMap::toString).sinkTo(control);
//
//    analyser1.union(analyser2)
//      .union(analyser3)
//      .union(analyser4)
//      .union(counter1.getSideOutput(toCoordinator))
//      .union(counter2.getSideOutput(toCoordinator))
//      .union(operator1.getSideOutput(toCoordinator))
//      .union(operator2.getSideOutput(toCoordinator))
//      .union(operator3.getSideOutput(toCoordinator))
//      .union(operator4.getSideOutput(toCoordinator))
//      .map(map -> map.toString())
//      .sinkTo(control);


    // generate complex events
//    operator1.keyBy(me -> 1).connect(operator2.keyBy(me -> 1)).process(
//      new KeyedCoProcessFunction<Long, Measurement, Measurement, Measurement>() {
//
//        ListState<Measurement> list1;
//        ListState<Measurement> list2;
//
//        @Override
//        public void open(OpenContext openContext) throws Exception {
//          list1 = getRuntimeContext().getListState(new ListStateDescriptor<>("list1", Measurement.class));
//          list2 = getRuntimeContext().getListState(new ListStateDescriptor<>("list2", Measurement.class));
//        }
//
//        @Override
//        public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
//          List<Measurement> listo = (List<Measurement>) list2.get();
//          if (!listo.isEmpty()) {
//            Measurement current = listo.get(0);
//            out.collect(new Measurement(1000, String.format("%s%n%s%n", value, current), 1));
//            list2.update(listo.subList(1, listo.size()));
//          } else {
//            list1.add(value);
//          }
//        }
//
//        @Override
//        public void processElement2(Measurement value, KeyedCoProcessFunction<Long, Measurement, Measurement, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
//          List<Measurement> listo = (List<Measurement>) list1.get();
//          if (!listo.isEmpty()) {
//            Measurement current = listo.get(0);
//            out.collect(new Measurement(1000, String.format("%s%n%s%n", value, current), 1));
//            list1.update(listo.subList(1, listo.size()));
//          } else {
//            list2.add(value);
//          }
//        }
//      }
//    ).print();

//    lambdaSource1.join(muOperator1)
//      .where(map -> map.get("batch"))
//      .equalTo(map -> map.get("batch"))
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
//      .apply(new JoinFunction<HashMap<String, Double>, HashMap<String, Double>, String>() {
//        @Override
//        public String join(HashMap<String, Double> first, HashMap<String, Double> second) throws Exception {
//          double total = first.get("total");
//          double result = 0.;
//          for (String key : new String[]{"0", "1", "2", "3"}) {
//            double weight = 0;
//            for (String key2 : new String[]{"11", "12"}) {
//              double share = Objects.requireNonNullElse(second.get("share" + key2 + "_" + key), 1.);
//              weight += share * second.get(key2);
//            }
//            result += (first.get(key) / (total == 0 ? 1 : total)) * weight;
//          }
//          double B = 1 / ((1 / result) - total);
//          return String.format("average processing time:%f%nB:%f%n%s%n%s%n", result, (B > 0 ? B : Double.NaN), first, second);
//        }
//      }).print();


//    lambdaSource1.keyBy(map -> map.get("batch")).connect(lambdaOperator1.keyBy(map -> map.get("batch"))).process(new KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, String>() {
//      MapState<String, Double> map1;
//      MapState<String, Double> map2;
//
//      @Override
//      public void open(Configuration conf) {
//        map1 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map1", String.class, Double.class));
//        map2 = getRuntimeContext().getMapState(new MapStateDescriptor<>("map2", String.class, Double.class));
//      }
//
//      @Override
//      public void processElement1(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, String>.Context ctx, Collector<String> out) throws Exception {
//        if (map2.isEmpty()) {
//          for (Map.Entry<String, Double> entry : value.entrySet()) {
//            map1.put(entry.getKey(), entry.getValue());
//          }
//        } else {
//          StringBuilder result = new StringBuilder();
//          for (Map.Entry<String, Double> entry : map2.entries()) {
//            result.append(entry).append(" ");
//          }
//          out.collect(String.format("%s%n%s%n", result, value));
//        }
//      }
//
//      @Override
//      public void processElement2(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, String>.Context ctx, Collector<String> out) throws Exception {
//        if (map1.isEmpty()) {
//          for (Map.Entry<String, Double> entry : value.entrySet()) {
//            map2.put(entry.getKey(), entry.getValue());
//          }
//        } else {
//          StringBuilder result = new StringBuilder();
//          for (Map.Entry<String, Double> entry : map1.entries()) {
//            result.append(entry).append(" ");
//          }
//          out.collect(String.format("%s%n%s%n", result, value));
//        }
//      }
//    }).print();

//    Pattern<Measurement, ?> pat = Pattern.<Measurement>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
//      .where(SimpleCondition.of(meas -> meas.machineId == 0))
//      .followedBy("middle")
//      .where(SimpleCondition.of(meas -> meas.machineId == 0))
//      .followedBy("end")
//      .where(SimpleCondition.of(meas -> meas.machineId == 1));
//
//    CEP.pattern(source1, pat).process(new PatternProcessFunction<Measurement, String>() {
//      @Override
//      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<String> out) throws Exception {
//        out.collect(match.get("start").get(0) + " " + match.get("middle") + " " + match.get("end"));
//      }
//    }).print();

//    KafkaSink<String> sink = KafkaSink.<String>builder()
//      .setBootstrapServers(kafkaAddress)
//      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//        .setTopic("sink1")
//        .setValueSerializationSchema(new SimpleStringSchema())
//        .build()
//      )
//      .build();
//
//    KafkaSource<String> source = KafkaSource.<String>builder()
//      .setBootstrapServers(kafkaAddress)
//      .setTopics("source1")
//      .setStartingOffsets(OffsetsInitializer.earliest())
//      .setValueOnlyDeserializer(new SimpleStringSchema())
//      .setBounded(OffsetsInitializer.latest())
//      .build();
//
//    DataStream<Measurement> s1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source1")
//      .map((MapFunction<String, Measurement>) value -> {
//        Gson deserializer = new GsonBuilder().registerTypeAdapter(Measurement.class, new EventJsonDeserializer()).create(); // define serializer
//        return deserializer.fromJson(value, Measurement.class);
//      }).assignTimestampsAndWatermarks(WatermarkStrategy.<Measurement>forMonotonousTimestamps().withTimestampAssigner((meas, t) -> meas.eventTime));
//
//
//    s1.keyBy(me -> 1L)
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(TimeUnit.SECONDS.toMillis(10))))
//      .process(new ProcessWindowFunction<Measurement, String, Long, TimeWindow>() {
//        @Override
//        public void process(Long aLong, ProcessWindowFunction<Measurement, String, Long, TimeWindow>.Context context, Iterable<Measurement> elements, Collector<String> out) throws Exception {
//          PriorityQueue<Measurement> zeros = new PriorityQueue<>(Comparator.comparingLong(meas -> meas.eventTime));
//          List<Measurement> listo = new ArrayList<>((Collection) elements);
//          for (Measurement meas : listo) {
//            if (meas.machineId == 0) {
//              zeros.add(meas);
//            }
//            if (meas.machineId == 1 && zeros.size() > 1) {
//              out.collect("C1 at :" + zeros.poll() + " " + zeros.poll() + " " + meas);
//            }
//          }
//        }
//      }).sinkTo(sink);
//
//    source1.keyBy(me -> me.eventTime / 100)
//


//    Pattern<Measurement, ?> pat = Pattern.<Measurement>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
//      .where(SimpleCondition.of(meas -> true))
//      .followedBy("middle")
//      .where(new IterativeCondition<Measurement>() {
//        @Override
//        public boolean filter(Measurement value, Context<Measurement> ctx) throws Exception {
//          return !value.equals(ctx.getEventsForPattern("start").iterator().next());
//        }
//      });
//
//    CEP.pattern(union, pat).process(new PatternProcessFunction<Measurement, String>() {
//      @Override
//      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<String> out) throws Exception {
//        out.collect(match.get("start").get(0) + " " + match.get("middle"));
//      }
//    }).print();


//    source1.union(source2).keyBy(meas -> meas.eventTime / 100).process(new ProcessFunction<Measurement, String>() {
//
//      ValueState<Measurement> last;
//
//      @Override
//      public void open(Configuration conf) {
//        last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Measurement.class));
//      }
//
//      @Override
//      public void processElement(Measurement value, ProcessFunction<Measurement, String>.Context ctx, Collector<String> out) throws Exception {
//        Measurement state = last.value();
//        if (state == null) {
//          last.update(value);
//        } else {
//          out.collect(state + " " + value);
//          last.clear();
//        }
//      }
//    }).print();

//    source1.join(source2)
//      .where(meas -> meas.eventTime / 10)
//      .equalTo(meas -> meas.eventTime / 10)
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(10)))
//      .apply((JoinFunction<Measurement, Measurement, ? extends Object>) (e1, e2) ->
//        e1 + " " + e2).print();

//    KeyedStream<Measurement, Long> out = source1.keyBy(meas -> meas.eventTime % 10);
//
//    out.process(new KeyedProcessFunction<Long, Measurement, String>() {
//      ValueState<Measurement> last;
//
//      @Override
//      public void open(Configuration conf) {
//        last = getRuntimeContext().getState(new ValueStateDescriptor<>("last", Measurement.class));
//      }
//
//      @Override
//      public void processElement(Measurement value, KeyedProcessFunction<Long, Measurement, String>.Context ctx, Collector<String> out) throws Exception {
//        Measurement state = last.value();
//        if (state == null) {
//          last.update(value);
//        } else {
//          out.collect(state + " " + value);
//        }
//      }
//    }).print();

//    Pattern<Measurement, ?> pattern = Pattern.<Measurement>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
//      .where(SimpleCondition.of(meas -> meas.eventTime > 0))
//      .followedBy("middle")
//      .where(new IterativeCondition<Measurement>() {
//
//        @Override
//        public boolean filter(Measurement value, Context<Measurement> ctx) throws Exception {
//          return value.eventTime > ctx.getEventsForPattern("start").iterator().next().eventTime;
//        }
//      })
//      .followedBy("end")
//      .where(new IterativeCondition<Measurement>() {
//
//        @Override
//        public boolean filter(Measurement value, Context<Measurement> ctx) throws Exception {
//          return value.eventTime > ctx.getEventsForPattern("middle").iterator().next().eventTime;
//        }
//      });
//    CEP.pattern(source1, pattern).process(new PatternProcessFunction<Measurement, String>() {
//      @Override
//      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<String> out) throws Exception {
//        StringBuilder result = new StringBuilder();
//        for (String key : match.keySet()) {
//          result.append(match.get(key));
//        }
//        out.collect(result.toString());
//      }
//    }).print();
//    DataStream<Measurement> source2 = env.fromSource(
//      sources.get(1),
//      WatermarkStrategy.<Measurement>forMonotonousTimestamps()
//        .withTimestampAssigner((meas, t) -> meas.eventTime),
//      "Generator").name("Source2").keyBy(meas -> 2);
//
//
//    final OutputTag<InputRate> outputTag = new OutputTag<>("side-output") {
//    };
//    final OutputTag<InputRate> outputTag2 = new OutputTag<>("side-output2") {
//    };
//    final OutputTag<InputRate> outputTag3 = new OutputTag<>("side-output3") {
//    };
//    final OutputTag<InputRate> outputTag4 = new OutputTag<>("side-output4") {
//    };
//
//    final OutputTag<String> complex1 = new OutputTag<>("complex1") {
//    };
//    final OutputTag<String> complex2 = new OutputTag<>("complex2") {
//    };
//    final OutputTag<String> complex3 = new OutputTag<>("complex3") {
//    };
//    final OutputTag<String> complex4 = new OutputTag<>("complex4") {
//    };
////     Basic Stream Job bellow
//
//    //TODO Find real benchmark or adapt using Milliseconds in the job - rate in which
//    // events are being created right now is too high
//
//    // Patterns are defined and matches are stored in pattern streams after being read
//    Pattern<Measurement, ?> pattern1 = PatternCreator.seq(10, 0, 0, 1);
//    Pattern<Measurement, ?> pattern2 = PatternCreator.seq(10, 1, 2, 3);
//    Pattern<Measurement, ?> pattern3 = PatternCreator.lazySeq(10);
//
//    SingleOutputStreamOperator<Measurement> shedder1 =
//      source1.process(new Shedder(outputTag)).name("Shedder1");
//    SingleOutputStreamOperator<Measurement> shedder2 =
//      source2.process(new Shedder(outputTag2)).name("Shedder2");
//    SingleOutputStreamOperator<Measurement> shedder3 =
//      source1.process(new Shedder(outputTag3)).name("Shedder3");
//    SingleOutputStreamOperator<Measurement> shedder4 =
//      source2.process(new Shedder(outputTag4)).name("Shedder4");
//
//    PatternStream<Measurement> Q11 = CEP.pattern(shedder1, pattern1);
//    PatternStream<Measurement> Q21 = CEP.pattern(shedder2, pattern1);
//    PatternStream<Measurement> Q12 = CEP.pattern(shedder3, pattern3);
//    PatternStream<Measurement> Q22 = CEP.pattern(shedder4, pattern2);
//
//    // Matches are processed and create new complex events C[1-4]
//    SingleOutputStreamOperator<ComplexEvent> C1 = Q11.process(new PatternProcessor("Q11", complex1)).name("Matches of Pattern1");
//    SingleOutputStreamOperator<ComplexEvent> C2 = Q21.process(new PatternProcessor("Q21", complex2)).name("Matches of Pattern2");
//    SingleOutputStreamOperator<ComplexEvent> C3 = Q12.process(new PatternProcessor("Q12", complex3)).name("Matches of Pattern3");
//    SingleOutputStreamOperator<ComplexEvent> C4 = Q22.process(new PatternProcessor("Q22", complex4)).name("Matches of Pattern4");
//
//    JoinFunction<ComplexEvent, ComplexEvent, ComplexEvent> join =
//      (first, second) -> new ComplexEvent("CE1: " + first.name + "    " + second.name,
//        Math.max(first.timestamp, second.timestamp));
//
//    // complex events are joined to produce outputs for the sinks
//    C1.join(C2)
//      .where(event -> 1)
//      .equalTo(event -> 1)
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
//      .apply(join)
//      .sinkTo(sinks.get(0)).name("Sink1");
//
//    C3.join(C4)
//      .where(event -> 2)
//      .equalTo(event -> 2)
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
//      .apply(join)
//      .sinkTo(sinks.get(1)).name("Sink2");
//
//    PrintSink<InputRate> collector = new PrintSink<>("Collector");
//
//    //TODO collect all of this data streams in a Kafka topic or similar construct
//    DataStream<InputRate> o1 = shedder1.getSideOutput(outputTag).keyBy(event -> 1);
//    DataStream<InputRate> o2 = shedder2.getSideOutput(outputTag2).keyBy(event -> 1);
//    DataStream<InputRate> o3 = shedder3.getSideOutput(outputTag3).keyBy(event -> 1);
//    DataStream<InputRate> o4 = shedder4.getSideOutput(outputTag4).keyBy(event -> 1);
//
//    DataStream<String> c1 = C1.getSideOutput(complex1).keyBy(event -> 1);
//    c1.process(new EventCollector()).name("Event Rates from C1");
//    DataStream<String> c2 = C2.getSideOutput(complex2).keyBy(event -> 1);
//    c2.process(new EventCollector()).name("Event Rates from C2");
//    DataStream<String> c3 = C3.getSideOutput(complex3).keyBy(event -> 1);
//    c3.process(new EventCollector()).name("Event Rates from C3");
//    DataStream<String> c4 = C4.getSideOutput(complex4).keyBy(event -> 1);
//    c4.process(new EventCollector()).name("Event Rates from C4");
//
    return env.execute("Flink Java CEP Prototype");
  }

  /*
   Mock method for testing adapting load shedding locally. In this variant, we write
   files to a specific depository every time a certain condition is met. A fileSource
   stream will continuously check the depository and consume the contents of every new
   file stored there. If we store rates for the input events in those files and
   connect the Measurement-source with the fileSource we can dynamically adjust how
   many events are consumed for each type.
  */

  /*
   Inherent problems:
   1) No guarantees to when the rate stream is going to be read
   2) Necessity to read and write files locally is per construction not scalable
  */

  //TODO implement this using Apache Kafka - Kafka topics are a better solution
  //TODO assign a shedder to each operator in order to control their load shedding
  // attributes
  public void testShedder(StreamExecutionEnvironment env,
                          DataStream<Measurement> source1,
                          DataStream<Measurement> source2) {
/*

    // connect to directory - look for new files
    FileSource<String> fileSource = FileSource.forRecordStreamFormat(
        new TextLineInputFormat(),
        new Path("/home/olaf-link/Documents/projects/p1/src/main/resources/test")
      )
      .monitorContinuously(Duration.ofSeconds(1)).build();

    DataStream<String> fromFile = env.fromSource(fileSource,
      WatermarkStrategy.noWatermarks(), "file-source").keyBy(s -> 1);

    // connect FileSource with MeasurementSource - store last rate-information in state
    // and use it to filter incoming Measurement events
    //TODO find a more reliable random number generator for the acceptance probability
    //TODO JUnit tests
    SingleOutputStreamOperator<Measurement> shedder1 =
      source1.process(new Shedder(outputTag));
//TODO create a shedder class and store the overall rate of events per second arriving
// at the operator or sink as well as the input rate of events per seconf of a specific
// event type T. Output should include the identification of the operator, the above
// mentioned rates and the ratio between the incoming rate of a type in comparison to
// the total incoming rate.
    SingleOutputStreamOperator<Measurement> shedder2 = source2.process(new Shedder(outputTag2));
    DataStream<Rates> uff2 = shedder2.getSideOutput(outputTag);
*/

  }
}
