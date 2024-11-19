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
import org.uni.potsdam.p1.actors.enrichers.Analyser;
import org.uni.potsdam.p1.actors.enrichers.Coordinator;
import org.uni.potsdam.p1.actors.operators.FSMOperator;
import org.uni.potsdam.p1.actors.operators.JoinCounter;
import org.uni.potsdam.p1.actors.operators.SinkLogger;
import org.uni.potsdam.p1.actors.operators.SourceCounter;
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

    // start execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // define a keyed data stream with which the operators send their information to the coordinator
    KeyedStream<String, Integer> global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global").keyBy(st -> 1);

    // define the data sources for the job
    DataStream<Measurement> source1 = env.fromSource(
        createMeasurementSource(RECORDS_PER_SECOND),
        WatermarkStrategy.noWatermarks(), "Generator1")
//      .slotSharingGroup("sources")
      .name("Source1");

    DataStream<Measurement> source2 = env.fromSource(
        createMeasurementSource(RECORDS_PER_SECOND),
        WatermarkStrategy.noWatermarks(), "Generator2")
//      .slotSharingGroup("sources")
      .name("Source2");

    // use EventCounters to measure the output rates of the sources per second and to forward the events to the OPERATORS
    SingleOutputStreamOperator<Measurement> counter1 = simpleConnect(source1, global)
      .process(new SourceCounter(OPERATORS[0])
        .setMetricsOutput("lambdaIn", toAnalyser1)
        .setMetricsOutput("sos", toCoordinator))
//      .slotSharingGroup("sources")
      .name("Counter1");

    SingleOutputStreamOperator<Measurement> counter2 = simpleConnect(source2, global)
      .process(new SourceCounter(OPERATORS[1])
        .setMetricsOutput("lambdaIn", toAnalyser2)
        .setMetricsOutput("sos", toCoordinator))
//      .slotSharingGroup("sources")
      .name("Counter2");

    // set OPERATORS 1 and 2 and collect both their processing (mu) and their output rates (lambda) on side outputs
    SingleOutputStreamOperator<Measurement> operator1 = simpleConnect(source1, global)
      .process(new FSMOperator(OPERATORS[0])
        .setSideOutput("12", toOperator4)
        .setMetricsOutput("ptime", toAnalyser1)
        .setMetricsOutput("sos", toCoordinator))
//      .slotSharingGroup("o1")
      .name("Operator1");

    SingleOutputStreamOperator<Measurement> operator2 = simpleConnect(source2, global)
      .process(new FSMOperator(OPERATORS[1])
        .setSideOutput("22", toOperator4)
        .setMetricsOutput("ptime", toAnalyser2)
        .setMetricsOutput("sos", toCoordinator))
//      .slotSharingGroup("o2")
      .name("Operator2");

    // gather the output rates of both operator 1 and 2, join the output rates which are relevant to the different OPERATORS downstream and forward the information to them
    SingleOutputStreamOperator<Measurement> counter3 = operator1.union(operator2)
      .process(new JoinCounter(OPERATORS[2])
        .setMetricsOutput("lambdaIn", toAnalyser3))
      .name("Counter3");

    SingleOutputStreamOperator<Measurement> counter4 = operator1.getSideOutput(toOperator4).union(operator2.getSideOutput(toOperator4))
      .process(new JoinCounter(OPERATORS[3])
        .setMetricsOutput("lambdaIn", toAnalyser4))
      .name("Counter4");

    // set OPERATORS 3 and 4 and collect both their processing (mu) and their output rates (lambda) on side outputs
    SingleOutputStreamOperator<Measurement> operator3 = simpleConnect(counter3, global)
      .process(new FSMOperator(OPERATORS[2])
        .setMetricsOutput("ptime", toAnalyser3)
        .setMetricsOutput("sos", toCoordinator))
      .name("Operator3");

    SingleOutputStreamOperator<Measurement> operator4 = simpleConnect(counter4, global)
      .process(new FSMOperator(OPERATORS[3])
        .setMetricsOutput("ptime", toAnalyser4)
        .setMetricsOutput("sos", toCoordinator))
      .name("Operator4");

    // log the system's output
    operator3.union(operator4).flatMap(new SinkLogger());

    // connect the stream of input rates with the stream of processing times for each operator, analyse their characteristics and contact the coordinator if necessary
    SingleOutputStreamOperator<Metrics> analyser1 =
      counter1.getSideOutput(toAnalyser1)
        .union(operator1.getSideOutput(toAnalyser1))
        .keyBy(map -> map.get("batch"))
        .process(new Analyser(OPERATORS[0]))
//      .slotSharingGroup("an")
        .name("Analyser1");

    SingleOutputStreamOperator<Metrics> analyser2 =
      counter2.getSideOutput(toAnalyser2)
        .union(operator2.getSideOutput(toAnalyser2))
        .keyBy(map -> map.get("batch"))
        .process(new Analyser(OPERATORS[1]))
//      .slotSharingGroup("an")
        .name("Analyser2");

    SingleOutputStreamOperator<Metrics> analyser3 =
      counter3.getSideOutput(toAnalyser3)
        .union(operator3.getSideOutput(toAnalyser3))
        .keyBy(map -> map.get("batch"))
        .process(new Analyser(OPERATORS[2]))
//      .slotSharingGroup("an")
        .name("Analyser3");

    SingleOutputStreamOperator<Metrics> analyser4 =
      counter4.getSideOutput(toAnalyser4)
        .union(operator4.getSideOutput(toAnalyser4))
        .keyBy(map -> map.get("batch"))
        .process(new Analyser(OPERATORS[3]))
//      .slotSharingGroup("an")
        .name("Analyser4");

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
      .keyBy(metric -> metric.id).process(new Coordinator(toKafka, LATENCY_BOUND, OPERATORS)).name("Coordinator");

    // store shedding rates in kafka
    coordinatorOutput.getSideOutput(toKafka).sinkTo(globalChannelOut);

    // output stream for debugging - returns the results of the coordinator
    coordinatorOutput.sinkTo(control);

    return env.execute("Flink Java CEP Prototype");
  }
}
