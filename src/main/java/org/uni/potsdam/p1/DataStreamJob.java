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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
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
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.*;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;

import java.util.ArrayList;
import java.util.List;


public class DataStreamJob {

  // define the number of sinks and sources
  private static final int NUMBER_OF_SINKS = 2;
  private static final int NUMBER_OF_SOURCES = 2;

  private static final int CONTROL_BATCH_SIZE = 100;
  // define the number of entries and
  private static final int BATCH_SIZE = 10_000;
  private static final GeneratorFunction<Long, Measurement>
    MEASUREMENT_GENERATOR1 = index -> new Measurement((int) (Math.random() * 4), 1);
  private static final GeneratorFunction<Long, Measurement>
    MEASUREMENT_GENERATOR2 = index -> new Measurement((int) (Math.random() * 4), 2);

  private final List<DataGeneratorSource<Measurement>> sources = new ArrayList<>();

  private static final String kafkaAddress = "kafka1:19092";

  OperatorInfo[] operators = new OperatorInfo[]{
    new OperatorInfo("o1",
      new String[]{"1", "2", "3", "0"},
      new EventPattern[]{
        new EventPattern("11", "SEQ:0|2:1|1", new String[]{"o3"}),
        new EventPattern("12", "AND:1:2:3", new String[]{"o4"})
      },
      false),

    new OperatorInfo("o2",
      new String[]{"1", "2", "3", "0"},
      new EventPattern[]{
        new EventPattern("21", "SEQ:0|2:1|1", new String[]{"o3"}),
        new EventPattern("22", "AND:1:2:3", new String[]{"o4"})
      },
      false),

    new OperatorInfo("o3",
      new String[]{"11", "21"},
      new EventPattern[]{
        new EventPattern("1000", "AND:11:21", null),
      },
      true),

    new OperatorInfo("o4",
      new String[]{"12", "22"},
      new EventPattern[]{
        new EventPattern("2000", "AND:12:22", null),
      },
      true)
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


    // define a keyed data stream with which the operators send their information to the coordinator
    KeyedStream<String, Integer> global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global").keyBy(s1 -> 1);


    // define the data sources for the job
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
    final OutputTag<Metrics> toJoiner = new OutputTag<>("out_to_joiner") {
    };
    final OutputTag<Metrics> toAnalyser2 = new OutputTag<>("out_to_analyser2") {
    };
//    final OutputTag<Metrics> outputOp2 = new OutputTag<>("out_of_op2") {
//    };
    final OutputTag<Metrics> toAnalyser3 = new OutputTag<>("out_to_analyser3") {
    };
    final OutputTag<Metrics> toAnalyser4 = new OutputTag<>("out_to_analyser4") {
    };
    final OutputTag<Measurement> toOperator4 = new OutputTag<>("out_to_operator4") {
    };


    // use EventCounters to measure the output rates of the sources per second and to forward the events to the operators
    SingleOutputStreamOperator<Measurement> counter1 = source1.keyBy(me -> 1).connect(global).process(new EventCounter("o1", new String[]{"0", "1", "2", "3"}, toAnalyser1, toCoordinator, CONTROL_BATCH_SIZE)).name("Counter1");
    SingleOutputStreamOperator<Measurement> counter2 = source2.keyBy(me -> 1).connect(global).process(new EventCounter("o2", new String[]{"0", "1", "2", "3"}, toAnalyser2, toCoordinator, CONTROL_BATCH_SIZE)).name("Counter2");


    // set operators 1 and 2 and collect both their processing (mu) and their output rates (lambda) on side outputs
    SingleOutputStreamOperator<Measurement> operator1 = counter1.keyBy(me -> 1).connect(global).process(new BasicOperator("o1", new String[]{"0", "1", "2", "3"}, new String[]{"11", "12"}, toOperator4, toAnalyser1, toJoiner, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator1");
    SingleOutputStreamOperator<Measurement> operator2 = counter2.keyBy(me -> 1).connect(global).process(new BasicOperator("o2", new String[]{"0", "1", "2", "3"}, new String[]{"21", "22"}, toOperator4, toAnalyser2, toJoiner, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator2");


    // gather the output rates of both operator 1 and 2, join the output rates which are relevant to the different operators downstream and forward the information to them
    SingleOutputStreamOperator<Metrics> joined = operator1.getSideOutput(toJoiner).keyBy(map -> map.get("batch")).connect(operator2.getSideOutput(toJoiner).keyBy(map -> map.get("batch"))).process(
      new EventJoiner(new String[]{"11", "21", "12", "22"}, new String[]{"21", "11", "22", "12"}, toAnalyser4)).name("Joined");


    // set operators 3 and 4 and collect both their processing (mu) and their output rates (lambda) on side outputs
    SingleOutputStreamOperator<Measurement> operator3 = operator1.union(operator2).keyBy(me -> 1).connect(global).process(new SinkOperator("o3", new String[]{"11", "21"}, new String[]{"1000"}, toAnalyser3, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator3");

    SingleOutputStreamOperator<Measurement> operator4 = operator1.getSideOutput(toOperator4).union(operator2.getSideOutput(toOperator4)).keyBy(me -> 1).connect(global).process(new SinkOperator("o4", new String[]{"12", "22"}, new String[]{"2000"}, toAnalyser4, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator4");


    // connect the stream of input rates with the stream of processing times for each operator, analyse their characteristics and contact the coordinator if necessary
    SingleOutputStreamOperator<Metrics> analyser1 = counter1.getSideOutput(toAnalyser1).keyBy(map -> map.get("batch"))
      .connect(operator1.getSideOutput(toAnalyser1).keyBy(map -> map.get("batch")))
      .process(new SCAnalyser("o1", new String[]{"1", "2", "3", "0"}, new String[]{"11", "12"}, toKafka))
      .name("Analyser1");

    SingleOutputStreamOperator<Metrics> analyser2 = counter2.getSideOutput(toAnalyser2).keyBy(map -> map.get("batch"))
      .connect(operator2.getSideOutput(toAnalyser2).keyBy(map -> map.get("batch")))
      .process(new SCAnalyser("o2", new String[]{"1", "2", "3", "0"}, new String[]{"21", "22"}, toKafka))
      .name("Analyser2");

    SingleOutputStreamOperator<Metrics> analyser3 = joined.keyBy(map -> map.get("batch"))
      .connect(operator3.getSideOutput(toAnalyser3).keyBy(map -> map.get("batch")))
      .process(new SCAnalyser("o3", new String[]{"11", "21"}, new String[]{"1000"}, toKafka)).name("Analyser3");

    SingleOutputStreamOperator<Metrics> analyser4 =
      joined.getSideOutput(toAnalyser4).keyBy(map -> map.get("batch"))
        .connect(operator4.getSideOutput(toAnalyser4).keyBy(map -> map.get("batch")))
        .process(new SCAnalyser("o4", new String[]{"12", "22"}, new String[]{"2000"}, toKafka)).name("Analyser4");


    // gather the outputs of all actors relevant to the coordinator
    DataStream<Metrics> streamToCoordinator = analyser1.union(analyser2)
      .union(analyser3)
      .union(analyser4)
      .union(counter1.getSideOutput(toCoordinator))
      .union(counter2.getSideOutput(toCoordinator))
      .union(operator1.getSideOutput(toCoordinator))
      .union(operator2.getSideOutput(toCoordinator))
      .union(operator3.getSideOutput(toCoordinator))
      .union(operator4.getSideOutput(toCoordinator));


    // execute coordinator
    SingleOutputStreamOperator<String> coordinatorOutput = streamToCoordinator
      .keyBy(metric -> metric.id).process(new Coordinator(toKafka, operators)).name("Coordinator");

    coordinatorOutput.sinkTo(control);

    // store shedding rates in kafka
    coordinatorOutput.getSideOutput(toKafka).sinkTo(globalChannelOut);


    return env.execute("Flink Java CEP Prototype");
  }
}
