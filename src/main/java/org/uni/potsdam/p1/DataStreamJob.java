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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Measurement;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
  HashMap<String, Double> map1;
  HashMap<String, Double> map2;
  String[] lambdaKeys;
  String[] muKeys;
  String groupName;

  OutputTag<String> sosOutput;
  ValueState<Double> lastAverage;

  public Analyser(String groupName, String[] lambdaKeys, String[] muKeys, OutputTag<String> sosOutput) {
    this.lambdaKeys = lambdaKeys;
    this.muKeys = muKeys;
    this.groupName = groupName;
    this.sosOutput = sosOutput;
  }

  @Override
  public void open(Configuration conf) {
    lastAverage = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAverage", Double.class));
  }

  @Override
  public void processElement1(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, HashMap<String, Double>>.Context ctx, Collector<HashMap<String, Double>> out) throws Exception {
    if (map2 == null) {
      map1 = value;
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
      double oldValue = Objects.requireNonNullElse(lastAverage.value(), 0.);
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      if (oldValue != 0.) {
        if (calculatedP / oldValue >= 0.1) {
          value.put("B", B);
          out.collect(value);
          map2.put("mu", 1 / calculatedP);
          out.collect(map2);
          ctx.output(sosOutput, groupName);
          map2 = null;
        }
        ctx.output(sosOutput, groupName + "B: " + B + "p: " + calculatedP);
      }
      lastAverage.update(calculatedP);
      double ratio = calculatedP / (oldValue == 0 ? 1 : oldValue);
      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio);
    }
  }

  @Override
  public void processElement2(HashMap<String, Double> value, KeyedCoProcessFunction<Double, HashMap<String, Double>, HashMap<String, Double>, HashMap<String, Double>>.Context ctx, Collector<HashMap<String, Double>> out) throws Exception {
    if (map1 == null) {
      map2 = value;
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
      double oldValue = Objects.requireNonNullElse(lastAverage.value(), 0.);
      double B = 1 / ((1 / (calculatedP == 0 ? 1 : calculatedP)) - total);
      if (oldValue != 0.) {
        if (calculatedP / oldValue >= 0.1) {
          map1.put("B", B);
          out.collect(map1);
          value.put("mu", 1 / calculatedP);
          out.collect(value);
          ctx.output(sosOutput, groupName);
          map1 = null;
        }
        ctx.output(sosOutput, groupName + "B: " + B + "p: " + calculatedP);
      }
      lastAverage.update(calculatedP);
      double ratio = calculatedP / (oldValue == 0 ? 1 : oldValue);
      ctx.output(sosOutput, groupName + " B: " + B + " p: " + calculatedP + " ratio: " + ratio);
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

  final int EVENT_BATCH = 100;
  boolean bound = false;
  double batch = 1;

  ComplexOperator(String groupName, String[] outKeys, OutputTag<HashMap<String, Double>> outputTagProcessingRates, OutputTag<HashMap<String, Double>> sosOutput) {
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
//    long time = System.currentTimeMillis();
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
    long ptimeC = System.currentTimeMillis() - value.eventTime;
    //TODO test time difference using the event times
    processingTime.put(outkey.toString(), processingTime.get(outkey.toString()) + ptimeC);
    long count = processingTime.get(total) + 1;
    processingTime.put(total, count);
    if (count % EVENT_BATCH == 0) {
      processingRates.put("batch", batch);
      outputRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, processingRates);
      batch++;
      for (String elem : outKeys) {
        processingMemory.put(elem, 0L);
        processingTime.put(elem, 0L);
        eventCounter.put(elem, 0L);
        memory.put(elem, 0L);
      }
      cycle = 1;
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

  final int EVENT_BATCH = 100;
  boolean bound = false;
  double batch = 1;

  Operator(String groupName, String[] keys, OutputTag<HashMap<String, Double>> outputTagProcessingRates, OutputTag<Measurement> secondOutput, OutputTag<HashMap<String, Double>> sosOutput) {
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
//    long ptimeQ11 = System.currentTimeMillis() - time;
    long ptimeQ11 = secondMeasurement - value.eventTime;

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
//    long ptimeQ12 = System.currentTimeMillis() - time;
    long ptimeQ12 = System.currentTimeMillis() - (secondMeasurement - time) - value.eventTime;

    processingTime.put(outkey1, processingTime.get(outkey1) + ptimeQ11);
    processingTime.put(outkey2, processingTime.get(outkey2) + ptimeQ12);

    long count = processingTime.get(total) + 1;
    processingTime.put(total, count);

    if (count % EVENT_BATCH == 0) {
      processingRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, processingRates);
      batch++;
      for (String elem : outKeys) {
        processingMemory.put(elem, 0L);
        processingTime.put(elem, 0L);
      }
      cycle = 1;
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
  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  OutputTag<HashMap<String, Double>> sosOutput;
  final int EVENT_BATCH = 100;
  double batch = 1;
  String groupName;

  Counter(String groupName, String[] keys, OutputTag<HashMap<String, Double>> output, OutputTag<HashMap<String, Double>> sosOutput) {
    this.keys = keys;
    for (String key : keys) {
      eventCounter.put(key, 0L);
      memory.put(key, 0L);
      outputRates.put(key, 0.);
    }
    outputTagProcessingRates = output;
    this.sosOutput = sosOutput;
    this.groupName = groupName;
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
    if (count % EVENT_BATCH == 0) {
      outputRates.put("batch", batch);
      ctx.output(outputTagProcessingRates, outputRates);
      batch++;
      for (String elem : keys) {
        memory.put(elem, 0L);
        eventCounter.put(elem, 0L);
      }
      cycle = 1;
    }
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
      double newValue = (double) (oldValue + (eventCounter.get(key) - oldValue)) / cycle;
      outputRates.put(key, newValue);
      memory.put(key, eventCounter.get(key));
    }
    cycle++;
  }
}

class SourceCounter extends ProcessFunction<Measurement, Measurement> {
  String[] keys;
  int cycle = 1;
  Map<String, Long> eventCounter = new HashMap<>(5);
  Map<String, Long> memory = new HashMap<>(5);
  HashMap<String, Double> outputRates = new HashMap<>(5);
  OutputTag<HashMap<String, Double>> outputTagProcessingRates;
  final int EVENT_BATCH = 100;
  double batch = 1;

  SourceCounter(OutputTag<HashMap<String, Double>> output, String[] keys) {
    this.keys = keys;
    for (String key : keys) {
      eventCounter.put(key, 0L);
      memory.put(key, 0L);
      outputRates.put(key, 0.);
    }
    outputTagProcessingRates = output;
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
//      double oldRate = outputRates.get(key);
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
    final OutputTag<HashMap<String, Double>> toCoordinator = new OutputTag<>("out_to_coordinator") {
    };
    final OutputTag<String> toKafka = new OutputTag<>("out_to_kafka") {
    };
    final OutputTag<HashMap<String, Double>> toAnalyser1 = new OutputTag<>("out_to_analyser1") {
    };
    final OutputTag<HashMap<String, Double>> toAnalyser2 = new OutputTag<>("out_to_analyser2") {
    };
    final OutputTag<HashMap<String, Double>> toAnalyser3 = new OutputTag<>("out_to_analyser3") {
    };
    final OutputTag<HashMap<String, Double>> toAnalyser4 = new OutputTag<>("out_to_analyser4") {
    };
    final OutputTag<Measurement> toOperator4 = new OutputTag<>("out_to_operator4") {
    };

    // count the output rates of the sources per second and forward the events to the operators
    SingleOutputStreamOperator<Measurement> counter1 = source1.keyBy(me -> 1).connect(global).process(new Counter("o1", new String[]{"0", "1", "2", "3", "total"}, toAnalyser1, toCoordinator));
    SingleOutputStreamOperator<Measurement> counter2 = source2.keyBy(me -> 1).connect(global).process(new Counter("o2", new String[]{"0", "1", "2", "3", "total"}, toAnalyser2, toCoordinator));

    DataStream<HashMap<String, Double>> lambdaSource1 = counter1.getSideOutput(toAnalyser1);
    DataStream<HashMap<String, Double>> lambdaSource2 = counter2.getSideOutput(toAnalyser2);


    // set operators 1 and 2 and collect both their processing (mu) and their output rates (lambda)
    SingleOutputStreamOperator<Measurement> operator1 = counter1.keyBy(me -> 1).connect(global).process(new Operator("o1", new String[]{"11", "12", "total"}, toAnalyser1, toOperator4, toCoordinator));
    DataStream<Measurement> operator1SecondOutput = operator1.getSideOutput(toOperator4);
    DataStream<HashMap<String, Double>> muOperator1 = operator1.getSideOutput(toAnalyser1);

    SingleOutputStreamOperator<Measurement> operator2 = counter2.keyBy(me -> 1).connect(global).process(new Operator("o2", new String[]{"21", "22", "total"}, toAnalyser2, toOperator4, toCoordinator));
    DataStream<Measurement> operator2SecondOutput = operator2.getSideOutput(toOperator4);
    DataStream<HashMap<String, Double>> muOperator2 = operator2.getSideOutput(toAnalyser2);

    // connect the stream of output rates of the sources with the stream of processing rates of the operators
    SingleOutputStreamOperator<HashMap<String, Double>> analyser1 = lambdaSource1.keyBy(map -> map.get("batch")).connect(muOperator1.keyBy(map -> map.get("batch")))
      .process(new Analyser("o1", new String[]{"0", "1", "2", "3"}, new String[]{"11", "12"}, toKafka));

    SingleOutputStreamOperator<HashMap<String, Double>> analyser2 = lambdaSource2.keyBy(map -> map.get("batch")).connect(muOperator2.keyBy(map -> map.get("batch")))
      .process(new Analyser("o2", new String[]{"0", "1", "2", "3"}, new String[]{"21", "22"}, toKafka));

    // count the input rates for operators 3 and 4
    SingleOutputStreamOperator<Measurement> counter3 = operator1.union(operator2).keyBy(me -> 1).connect(global).process(new Counter("o3", new String[]{"11", "21", "total"}, toAnalyser3, toCoordinator));
    SingleOutputStreamOperator<Measurement> counter4 = operator1SecondOutput.union(operator2SecondOutput).keyBy(me -> 1).connect(global).process(new Counter("o4", new String[]{"12", "22", "total"}, toAnalyser4, toCoordinator));

    // set operators 3 and 4 and collect both their processing (mu) and their output rates (lambda)
    SingleOutputStreamOperator<Measurement> operator3 = counter3.keyBy(me -> 1).connect(global).process(
      new ComplexOperator("o3", new String[]{"1000", "total"}, toAnalyser3, toCoordinator));
    DataStream<HashMap<String, Double>> muOperator3 = operator3.getSideOutput(toAnalyser3);

    SingleOutputStreamOperator<Measurement> operator4 = counter4.keyBy(me -> 1).connect(global).process(
      new ComplexOperator("o4", new String[]{"2000", "total"}, toAnalyser4, toCoordinator));
    DataStream<HashMap<String, Double>> muOperator4 = operator4.getSideOutput(toAnalyser4);

    // connect the stream of output rates of the sources with the stream of processing rates of the operators
    SingleOutputStreamOperator<HashMap<String, Double>> analyser3 = counter3.getSideOutput(toAnalyser3).keyBy(map -> map.get("batch")).connect(muOperator3.keyBy(map -> map.get("batch")))
      .process(new Analyser("o3", new String[]{"11", "21"}, new String[]{"1000"}, toKafka));

    SingleOutputStreamOperator<HashMap<String, Double>> analyser4 = counter4.getSideOutput(toAnalyser4).keyBy(map -> map.get("batch")).connect(muOperator4.keyBy(map -> map.get("batch")))
      .process(new Analyser("o4", new String[]{"12", "22"}, new String[]{"2000"}, toKafka));

    analyser1.union(analyser2).union(analyser3).union(analyser4).map(HashMap::toString).sinkTo(globalChannelOut);
//    analyser1.getSideOutput(toKafka)
//      .union(analyser2.getSideOutput(toKafka))
//      .union(analyser3.getSideOutput(toKafka))
//      .union(analyser4.getSideOutput(toKafka))
//      .sinkTo(control);

    analyser1.union(analyser2)
      .union(analyser3)
      .union(analyser4)
      .union(counter1.getSideOutput(toCoordinator))
      .union(counter2.getSideOutput(toCoordinator))
      .union(counter3.getSideOutput(toCoordinator))
      .union(counter4.getSideOutput(toCoordinator))
      .union(operator1.getSideOutput(toCoordinator))
      .union(operator2.getSideOutput(toCoordinator))
      .union(operator3.getSideOutput(toCoordinator))
      .union(operator4.getSideOutput(toCoordinator))
      .map(map -> map.toString())
      .sinkTo(control);

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
