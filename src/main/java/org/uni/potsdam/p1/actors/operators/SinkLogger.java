package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.Measurement;

/**
 * Sink class to log the outputs of the system
 */
public class SinkLogger extends RichFlatMapFunction<Measurement, Object> {

  private final Logger sinkLog = LoggerFactory.getLogger("sinkLog");

  @Override
  public void flatMap(Measurement value, Collector<Object> out) throws Exception {
    sinkLog.info(value.toJson(value.getTypeAsKey()));
  }
}
