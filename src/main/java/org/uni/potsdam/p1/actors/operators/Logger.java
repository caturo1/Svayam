package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.Measurement;

/**
 * Logs events
 */
public class Logger implements MapFunction<Measurement, Measurement> {

  String name;

  public Logger(String name) {
    this.name = name;
    log = LoggerFactory.getLogger(name + "log");
  }

  private final org.slf4j.Logger log;

  @Override
  public Measurement map(Measurement value) throws Exception {
    if (name.isEmpty()) {
      name = value.getTypeAsKey();
    }
    log.info(value.toJson(name));
    return value;
  }
}
