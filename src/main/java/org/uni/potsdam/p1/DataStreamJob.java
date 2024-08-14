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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DataStreamJob {

  public static int BATCH_SIZE = 10000;
  public static long START_TIME = System.nanoTime();

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();
    doExample1(env);
    env.execute("Flink Java API Skeleton");
  }

  private static void doExample1(StreamExecutionEnvironment env) {
    GeneratorFunction<Long, Tuple2<Character, Long>> uff =
      index -> new Tuple2<>((char) (97 + Math.random() * 26), System.nanoTime());

    DataGeneratorSource<Tuple2<Character, Long>> source =
      new DataGeneratorSource<>(uff, BATCH_SIZE,
        TupleTypeInfo.getBasicTupleTypeInfo(Character.class, Long.class));

    DataStream<Tuple2<Character, Long>> in = env.fromSource(source,
      WatermarkStrategy.<Tuple2<Character, Long>>forMonotonousTimestamps()
        .withTimestampAssigner((tuple, t) -> tuple.f1),
      "Generator");

    Pattern<Tuple2<Character, Long>, ?> pattern = Pattern.<Tuple2<Character, Long>>begin("start")
      .where(SimpleCondition.of(c -> c.f0 == 'c'))
      .followedBy("middle")
      .where(SimpleCondition.of(c -> c.f0 == 'a'))
      .within(Duration.ofSeconds(20))
      .followedBy("end")
      .where(SimpleCondition.of(c -> c.f0 == 'r'))
      .within(Duration.ofSeconds(20));

    CEP.pattern(in, pattern).process(new PatternProcessFunction<Tuple2<Character, Long>, String>() {
      @Override
      public void processMatch(Map<String, List<Tuple2<Character, Long>>> match, Context ctx, Collector<String> out) throws Exception {
        out.collect("CAR");
      }
    }).print();
  }
}
