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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {

  // define the number of sinks and sources
  private static final int NUMBER_OF_SINKS = 2;
  private static final int NUMBER_OF_SOURCES = 2;

  // define the number of entries and
  private static final int BATCH_SIZE = 1000000;
  private static final GeneratorFunction<Long, Measurement>
    MEASUREMENT_GENERATOR = Measurement::new;

  private final List<DataGeneratorSource<Measurement>> sources = new ArrayList<>();
  private final List<Sink<ComplexEvent>> sinks = new ArrayList<>();

  public DataStreamJob(int numberOfSources, int numberOfSinks) {
    for (int i = 0; i < numberOfSources; i++) {
      sources.add(new DataGeneratorSource<>(
        MEASUREMENT_GENERATOR,
        BATCH_SIZE,
        TypeInformation.of(Measurement.class)));
    }
    for (int i = 0; i < numberOfSinks; i++) {
      sinks.add(new PrintSink<>());
    }
  }

  public static void main(String[] args) throws Exception {
    DataStreamJob job = new DataStreamJob(NUMBER_OF_SOURCES, NUMBER_OF_SINKS);
    job.execute();
  }

  public JobExecutionResult execute() throws Exception {
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Measurement> source1 = env.fromSource(
      sources.get(0),
      WatermarkStrategy.<Measurement>forMonotonousTimestamps()
        .withTimestampAssigner((meas, t) -> meas.eventTime),
      "Generator").name("Source1").keyBy(meas -> 1);

    DataStream<Measurement> source2 = env.fromSource(
      sources.get(1),
      WatermarkStrategy.<Measurement>forMonotonousTimestamps()
        .withTimestampAssigner((meas, t) -> meas.eventTime),
      "Generator").name("Source2").keyBy(meas -> 2);


    final OutputTag<InputRate> outputTag = new OutputTag<>("side-output") {
    };
    final OutputTag<InputRate> outputTag2 = new OutputTag<>("side-output2") {
    };
    final OutputTag<InputRate> outputTag3 = new OutputTag<>("side-output3") {
    };
    final OutputTag<InputRate> outputTag4 = new OutputTag<>("side-output4") {
    };

    final OutputTag<String> complex1 = new OutputTag<>("complex1") {
    };
    final OutputTag<String> complex2 = new OutputTag<>("complex2") {
    };
    final OutputTag<String> complex3 = new OutputTag<>("complex3") {
    };
    final OutputTag<String> complex4 = new OutputTag<>("complex4") {
    };
//     Basic Stream Job bellow

    //TODO Find real benchmark or adapt using Milliseconds in the job - rate in which
    // events are being created right now is too high

    // Patterns are defined and matches are stored in pattern streams after being read
    Pattern<Measurement, ?> pattern1 = PatternCreator.seq(10, 0, 0, 1);
    Pattern<Measurement, ?> pattern2 = PatternCreator.seq(10, 1, 2, 3);
    Pattern<Measurement, ?> pattern3 = PatternCreator.lazySeq(10);

    SingleOutputStreamOperator<Measurement> shedder1 =
      source1.process(new Shedder(outputTag)).name("Shedder1");
    SingleOutputStreamOperator<Measurement> shedder2 =
      source2.process(new Shedder(outputTag2)).name("Shedder2");
    SingleOutputStreamOperator<Measurement> shedder3 =
      source1.process(new Shedder(outputTag3)).name("Shedder3");
    SingleOutputStreamOperator<Measurement> shedder4 =
      source2.process(new Shedder(outputTag4)).name("Shedder4");

    PatternStream<Measurement> Q11 = CEP.pattern(shedder1, pattern1);
    PatternStream<Measurement> Q21 = CEP.pattern(shedder2, pattern1);
    PatternStream<Measurement> Q12 = CEP.pattern(shedder3, pattern3);
    PatternStream<Measurement> Q22 = CEP.pattern(shedder4, pattern2);

    // Matches are processed and create new complex events C[1-4]
    SingleOutputStreamOperator<ComplexEvent> C1 = Q11.process(new PatternProcessor("Q11", complex1)).name("Matches of Pattern1");
    SingleOutputStreamOperator<ComplexEvent> C2 = Q21.process(new PatternProcessor("Q21", complex2)).name("Matches of Pattern2");
    SingleOutputStreamOperator<ComplexEvent> C3 = Q12.process(new PatternProcessor("Q12", complex3)).name("Matches of Pattern3");
    SingleOutputStreamOperator<ComplexEvent> C4 = Q22.process(new PatternProcessor("Q22", complex4)).name("Matches of Pattern4");

    JoinFunction<ComplexEvent, ComplexEvent, ComplexEvent> join =
      (first, second) -> new ComplexEvent("CE1: " + first.name + "    " + second.name,
        Math.max(first.timestamp, second.timestamp));

    // complex events are joined to produce outputs for the sinks
    C1.join(C2)
      .where(event -> 1)
      .equalTo(event -> 1)
      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
      .apply(join)
      .sinkTo(sinks.get(0)).name("Sink1");

    C3.join(C4)
      .where(event -> 2)
      .equalTo(event -> 2)
      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
      .apply(join)
      .sinkTo(sinks.get(1)).name("Sink2");

    PrintSink<InputRate> collector = new PrintSink<>("Collector");

    //TODO collect all of this data streams in a Kafka topic or similar construct
    DataStream<InputRate> o1 = shedder1.getSideOutput(outputTag).keyBy(event -> 1);
    DataStream<InputRate> o2 = shedder2.getSideOutput(outputTag2).keyBy(event -> 1);
    DataStream<InputRate> o3 = shedder3.getSideOutput(outputTag3).keyBy(event -> 1);
    DataStream<InputRate> o4 = shedder4.getSideOutput(outputTag4).keyBy(event -> 1);

    DataStream<String> c1 = C1.getSideOutput(complex1).keyBy(event -> 1);
    c1.process(new EventCollector()).name("Event Rates from C1");
    DataStream<String> c2 = C2.getSideOutput(complex2).keyBy(event -> 1);
    c2.process(new EventCollector()).name("Event Rates from C2");
    DataStream<String> c3 = C3.getSideOutput(complex3).keyBy(event -> 1);
    c3.process(new EventCollector()).name("Event Rates from C3");
    DataStream<String> c4 = C4.getSideOutput(complex4).keyBy(event -> 1);
    c4.process(new EventCollector()).name("Event Rates from C4");

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
