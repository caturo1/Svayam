package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

public class TestOperator
  extends CoProcessFunction<Event,String, Event> {

  TestCore core;

  public TestOperator(OperatorInfo operatorInfo) {
    core = new TestCore(operatorInfo);
  }

  @Override
  public void processElement1(Event value, CoProcessFunction<Event, String, Event>.Context ctx, Collector<Event> out) {
    core.processWithContext(value, ctx);
  }

  @Override
  public void processElement2(String value, CoProcessFunction<Event, String, Event>.Context ctx, Collector<Event> out) {
    core.processMessages(value, ctx);
  }

  public TestOperator setSideOutput(String operatorName, EventOutput whereTo) {
    core.setSideOutput(operatorName, whereTo);
    return this;
  }

  public TestOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
    core.setMetricsOutput(metric, whereTo);
    return this;
  }

}
