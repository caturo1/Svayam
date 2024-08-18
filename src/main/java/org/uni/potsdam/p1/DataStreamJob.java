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
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
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
    MEASUREMENT_GENERATOR = index -> new Measurement();

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
      "Generator");

    DataStream<Measurement> source2 = env.fromSource(
      sources.get(1),
      WatermarkStrategy.<Measurement>forMonotonousTimestamps()
        .withTimestampAssigner((meas, t) -> meas.eventTime),
      "Generator");
    Pattern<Measurement, ?> pattern1 = PatternCreator.seq(1000000, 0, 0, 1);
    Pattern<Measurement, ?> pattern2 = PatternCreator.seq(1000000, 1, 2, 3);
    Pattern<Measurement, ?> pattern3 = PatternCreator.lazySeq(1000000);

    PatternStream<Measurement> Q11 = CEP.pattern(source1, pattern1);
    PatternStream<Measurement> Q21 = CEP.pattern(source2, pattern1);
//    PatternStream<Measurement> Q12 = CEP.pattern(source1, pattern3);
//    PatternStream<Measurement> Q22 = CEP.pattern(source2, pattern2);

    DataStream<ComplexEvent> C1 = Q11.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        String result = "Q11 -" +
          " Duration : " + Duration.between(
          LocalTime.ofNanoOfDay(match.get("start").get(0).eventTime),
          LocalTime.ofNanoOfDay(match.get("end").get(0).eventTime))
          + " " + ctx.timestamp();
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });
    DataStream<ComplexEvent> C2 = Q21.process(new PatternProcessFunction<>() {
      @Override
      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) {
        String result = "Q21 -" +
          " Duration : " + Duration.between(
          LocalTime.ofNanoOfDay(match.get("start").get(0).eventTime),
          LocalTime.ofNanoOfDay(match.get("end").get(0).eventTime))
          + " " + ctx.timestamp();
        out.collect(new ComplexEvent(result, ctx.timestamp()));
      }
    });
//    DataStream<ComplexEvent> C3 = Q12.process(new PatternProcessFunction<>() {
//      @Override
//      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) throws Exception {
//        out.collect(new ComplexEvent("Q12", ctx.timestamp()));
//      }
//    });
//    DataStream<ComplexEvent> C4 = Q22.process(new PatternProcessFunction<>() {
//      @Override
//      public void processMatch(Map<String, List<Measurement>> match, Context ctx, Collector<ComplexEvent> out) throws Exception {
//        out.collect(new ComplexEvent("Q22", ctx.timestamp()));
//      }
//    });

    C1.join(C2)
      .where(event -> 1)
      .equalTo(event -> 1)
      .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
      .apply(
        (JoinFunction<ComplexEvent, ComplexEvent, String>)
          (first, second) -> first.name + " -- " + second.name).print();

//    C1.join(C2)
//      .where(event -> 1)
//      .equalTo(event -> 1)
//      .window(TumblingEventTimeWindows.of(Duration.ofMillis(10000)))
//      .apply(
//        (JoinFunction<ComplexEvent, ComplexEvent, String>)
//          (first, second) -> first.name + " -- " + second.name).print();

    return env.execute("Flink Java CEP Prototype");
  }
}
