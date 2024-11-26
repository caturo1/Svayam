package org.uni.potsdam.p1.actors.sources;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.Measurement;

public class SourceLogger extends ProcessFunction<Measurement, Object> {
  Logger sourceLog = LoggerFactory.getLogger("sourceLog");
  String name;

  public SourceLogger(String name) {
    this.name = name;
  }

  @Override
  public void processElement(Measurement value, ProcessFunction<Measurement, Object>.Context ctx, Collector<Object> out) throws Exception {
    sourceLog.info(value.toJson(name));
  }
}
