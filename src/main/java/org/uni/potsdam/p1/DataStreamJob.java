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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.actors.*;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.Settings;


/**
 * <p>
 * This class holds the Flink-Query to be executed. Here we define sources, operators and
 * sinks as well as the order in which the data is processed. Define the query in the
 * execute method and call it in the main method.
 * </p>
 */
public class DataStreamJob extends Settings {

  public static void main(String[] args) throws Exception {
    DataStreamJob job = new DataStreamJob();
    job.execute();
  }

  @Override
  public JobExecutionResult execute() throws Exception {

    // Define side output channels for the operators
    final OutputTag<Metrics> toCoordinator = new OutputTag<>("out_to_coordinator") {
    };
    final OutputTag<String> toKafka = new OutputTag<>("out_to_kafka") {
    };
    final OutputTag<Metrics> toAnalyser1 = new OutputTag<>("out_to_analyser1") {
    };
    final OutputTag<Metrics> toAnalyser2 = new OutputTag<>("out_to_analyser2") {
    };
    final OutputTag<Metrics> toAnalyser3 = new OutputTag<>("out_to_analyser3") {
    };
    final OutputTag<Metrics> toAnalyser4 = new OutputTag<>("out_to_analyser4") {
    };
    final OutputTag<Metrics> toJoiner = new OutputTag<>("out_to_joiner") {
    };
    final OutputTag<Measurement> toOperator4 = new OutputTag<>("out_to_operator4") {
    };

    // start execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // define a keyed data stream with which the operators send their information to the coordinator
    KeyedStream<String, Integer> global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global").keyBy(st -> 1);

    // define the data sources for the job
    DataStream<Measurement> source1 = env.fromSource(
      createMeasuremetSource(100),
      WatermarkStrategy.noWatermarks(),
      "Generator1").name("Source1");

    DataStream<Measurement> source2 = env.fromSource(
      createMeasuremetSource(100),
      WatermarkStrategy.noWatermarks(),
      "Generator2").name("Source2");


    // use EventCounters to measure the output rates of the sources per second and to forward the events to the OPERATORS
    SingleOutputStreamOperator<Measurement> counter1 = simpleConnect(source1, global).process(new EventCounter("o1", new String[]{"0", "1", "2", "3"}, toAnalyser1, toCoordinator, CONTROL_BATCH_SIZE)).name("Counter1");
    SingleOutputStreamOperator<Measurement> counter2 = simpleConnect(source2, global).process(new EventCounter("o2", new String[]{"0", "1", "2", "3"}, toAnalyser2, toCoordinator, CONTROL_BATCH_SIZE)).name("Counter2");


    // set OPERATORS 1 and 2 and collect both their processing (mu) and their output rates (lambda) on side outputs
    SingleOutputStreamOperator<Measurement> operator1 = simpleConnect(counter1, global).process(new BasicOperator("o1", new String[]{"0", "1", "2", "3"}, new String[]{"11", "12"}, toOperator4, toAnalyser1, toJoiner, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator1");
    SingleOutputStreamOperator<Measurement> operator2 = simpleConnect(counter2, global).process(new BasicOperator("o2", new String[]{"0", "1", "2", "3"}, new String[]{"21", "22"}, toOperator4, toAnalyser2, toJoiner, toCoordinator, CONTROL_BATCH_SIZE)).name("Operator2");


    // gather the output rates of both operator 1 and 2, join the output rates which are relevant to the different OPERATORS downstream and forward the information to them
    SingleOutputStreamOperator<Metrics> joined = operator1.getSideOutput(toJoiner).keyBy(map -> map.get("batch")).connect(operator2.getSideOutput(toJoiner).keyBy(map -> map.get("batch"))).process(
      new EventJoiner(new String[]{"11", "21", "12", "22"}, new String[]{"21", "11", "22", "12"}, toAnalyser4)).name("Joined");


    // set OPERATORS 3 and 4 and collect both their processing (mu) and their output rates (lambda) on side outputs
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
      .keyBy(metric -> metric.id).process(new Coordinator(toKafka, OPERATORS)).name("Coordinator");

    coordinatorOutput.sinkTo(control);

    // store shedding rates in kafka
    coordinatorOutput.getSideOutput(toKafka).sinkTo(globalChannelOut);


    return env.execute("Flink Java CEP Prototype");
  }
}
