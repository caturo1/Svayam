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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.ComplexEvent;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.PatternCreator;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataStreamJob {

  // define the number of sinks and sources
  private static final int NUMBER_OF_SINKS = 2;
  private static final int NUMBER_OF_SOURCES = 2;

  // define the number of entries and
  private static final int BATCH_SIZE = 10000;
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
      "Generator").keyBy(meas -> 1);

    DataStream<Measurement> source2 = env.fromSource(
      sources.get(1),
      WatermarkStrategy.<Measurement>forMonotonousTimestamps()
        .withTimestampAssigner((meas, t) -> meas.eventTime),
      "Generator").keyBy(meas -> 2);


//     Basic Stream Job bellow

    //TODO Find real benchmark or adapt using Milliseconds in the job - rate in which
    // events are being created right now is too high

    // Patterns are defined and matches are stored in pattern streams after being read
    Pattern<Measurement, ?> pattern1 = PatternCreator.seq(3, 0, 0, 1);
    Pattern<Measurement, ?> pattern2 = PatternCreator.seq(100000, 1, 2, 3);
    Pattern<Measurement, ?> pattern3 = PatternCreator.lazySeq(100000);

//    PatternStream<Measurement> Q11 = CEP.pattern(shedder1, pattern1);
//    PatternStream<Measurement> Q21 = CEP.pattern(shedder2, pattern1);
    PatternStream<Measurement> Q11 = CEP.pattern(source1, pattern1);
    PatternStream<Measurement> Q21 = CEP.pattern(source2, pattern1);
    PatternStream<Measurement> Q12 = CEP.pattern(source1, pattern3);
    PatternStream<Measurement> Q22 = CEP.pattern(source2, pattern2);

    // Matches are processed and create new complex events C[1-4]
    DataStream<ComplexEvent> C1 = Q11.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        String result = "Q11 -" +
          " D : " + match.get("start").get(0).eventTime + " " +
          match.get("middle").get(0).eventTime + " " +
          match.get("end").get(0).eventTime;
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });
    DataStream<ComplexEvent> C2 = Q21.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        Measurement start = match.get("start").get(0);
        Measurement middle = match.get("middle").get(0);
        Measurement end = match.get("end").get(0);
        String result = "Q21 -" +
          " D : " + start.eventTime + " " +
          middle.eventTime + " " +
          end.eventTime;
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });
    DataStream<ComplexEvent> C3 = Q12.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        Measurement one = match.get("start").get(0);
        Measurement two = match.get("middle").get(0);
        Measurement three = match.get("end").get(0);
        String result = "Q12 -" +
          " D : " + Duration.between(
          LocalTime.ofNanoOfDay(one.eventTime),
          LocalTime.ofNanoOfDay(three.eventTime))
          + " P: " + one.machineId + " " + two.machineId + " " + three.machineId;
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });
    DataStream<ComplexEvent> C4 = Q22.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        String result = "Q22 -" +
          " D : " + Duration.between(
          LocalTime.ofNanoOfDay(match.get("start").get(0).eventTime),
          LocalTime.ofNanoOfDay(match.get("end").get(0).eventTime));
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });

    // complex events are joined to produce outputs for the sinks
    C1.join(C2)
      .where(event -> 1)
      .equalTo(event -> 1)
      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1)))
      .apply(
        (JoinFunction<ComplexEvent, ComplexEvent, ComplexEvent>)
          (first, second) -> new ComplexEvent("CE1: " + first.name + "    " + second.name,
            Math.max(first.timestamp, second.timestamp)))
      .sinkTo(sinks.get(0));

    C3.join(C4)
      .where(event -> 2)
      .equalTo(event -> 2)
      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
      .apply(
        (JoinFunction<ComplexEvent, ComplexEvent, ComplexEvent>)
          (first, second) -> new ComplexEvent("CE2: " + first.name + "    " + second.name,
            first.timestamp))
      .sinkTo(sinks.get(1));

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

    // connect to directory - look for new files
    FileSource<String> fileSource = FileSource.forRecordStreamFormat(
        new TextLineInputFormat(),
        new Path("/home/olaf-link/Documents/projects/p1/src/main/resources/test")
      )
      .monitorContinuously(Duration.ofSeconds(1)).build();

    final OutputTag<String> outputTag = new OutputTag<>("side-output") {
    };
    DataStream<String> fromFile = env.fromSource(fileSource,
      WatermarkStrategy.noWatermarks(), "file-source").keyBy(s -> 1);

    final OutputTag<String> outputTag2 = new OutputTag<>("side-output2") {
    };
    // connect FileSource with MeasurementSource - store last rate-information in state
    // and use it to filter incoming Measurement events
    //TODO find a more reliable random number generator for the acceptance probability
    //TODO JUnit tests
    SingleOutputStreamOperator<Measurement> shedder1 = source1.process(
      new ProcessFunction<>() {
        ValueState<Rates> rates;
        ValueState<Integer> count;
        ValueState<LocalTime> time;

        @Override
        public void open(OpenContext openContext) {
          rates = getRuntimeContext().getState(new ValueStateDescriptor<>("rates",
            Rates.class));
          count = getRuntimeContext().getState(new ValueStateDescriptor<>("count",
            Integer.class));
          time = getRuntimeContext().getState(new ValueStateDescriptor<>("time",
            LocalTime.class));
        }

        @Override
        public void processElement(Measurement value, Context ctx,
                                   Collector<Measurement> out) throws Exception {
          if (count.value() == null) {
            count.update(1);
            time.update(LocalTime.now());
          } else if (count.value() % 10 != 0) {
            count.update(1 + count.value());
          } else {
            ctx.output(outputTag,
              String.valueOf(count.value()) + Duration.between(
                time.value(),
                LocalTime.now()));
            count.update(1 + count.value());
            time.update(LocalTime.now());
          }
          double prob = Math.random();
          Rates current = rates.value();
          if (
            current == null ||
              value.machineId == 0 && prob < current.E1 ||
              value.machineId == 1 && prob < current.E2 ||
              value.machineId == 2 && prob < current.E3 ||
              value.machineId == 3 && prob < current.E4
          ) {
//            System.out.println(
//              "Yep: machine: " + which + " - prob: " + prob + " current:" + (current == null ? 1 :
//                current.getEvent(which)));
            out.collect(value);
          }
//          else {
//            System.out.println(
//              "Nope: machine: " + which + " - prob: " + current.getEvent(which));
//          }
        }
      }
    );
//TODO create a shedder class and store the overall rate of events per second arriving
// at the operator or sink as well as the input rate of events per seconf of a specific
// event type T. Output should include the identification of the operator, the above
// mentioned rates and the ratio between the incoming rate of a type in comparison to
// the total incoming rate.
    SingleOutputStreamOperator<Measurement> shedder2 = source2.process(
      new ProcessFunction<>() {
        ValueState<Rates> rates;
        ValueState<Integer> count;
        ValueState<LocalTime> time;

        @Override
        public void open(OpenContext openContext) {
          rates = getRuntimeContext().getState(new ValueStateDescriptor<>("rates",
            Rates.class));
          count = getRuntimeContext().getState(new ValueStateDescriptor<>("count",
            Integer.class));
          time = getRuntimeContext().getState(new ValueStateDescriptor<>("time",
            LocalTime.class));
        }

        @Override
        public void processElement(Measurement value, Context ctx,
                                   Collector<Measurement> out) throws Exception {
          if (count.value() == null) {
            count.update(1);
            time.update(LocalTime.now());
          } else if (count.value() % 10 != 0) {
            count.update(1 + count.value());
          } else {
            ctx.output(outputTag2,
              String.valueOf(count.value()) + Duration.between(
                time.value(),
                LocalTime.now()));
            count.update(1 + count.value());
            time.update(LocalTime.now());
          }
          double prob = Math.random();
          Rates current = rates.value();
          if (
            current == null ||
              value.machineId == 0 && prob < current.E1 ||
              value.machineId == 1 && prob < current.E2 ||
              value.machineId == 2 && prob < current.E3 ||
              value.machineId == 3 && prob < current.E4
          ) {
//            System.out.println(
//              "Yep: machine: " + which + " - prob: " + prob + " current:" + (current == null ? 1 :
//                current.getEvent(which)));
            out.collect(value);
          }
//          else {
//            System.out.println(
//              "Nope: machine: " + which + " - prob: " + current.getEvent(which));
//          }
        }
      }
    );

    DataStream<String> uff = shedder1.getSideOutput(outputTag);
    DataStream<String> uff2 = shedder2.getSideOutput(outputTag);

  }
}
