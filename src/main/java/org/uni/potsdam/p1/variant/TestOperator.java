package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

public class TestOperator extends CoProcessFunction<Measurement,String,Measurement> {

  TestCore core;

  public TestOperator(OperatorInfo operatorInfo) {
    core = new TestCore(operatorInfo);
  }

  @Override
  public void processElement1(Measurement value, CoProcessFunction<Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    core.processWithContext(value, ctx);
  }

  @Override
  public void processElement2(String value, CoProcessFunction<Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    core.processMessages(value, ctx);
  }

  public TestOperator setSideOutput(String operatorName, MeasurementOutput whereTo) {
    core.setSideOutput(operatorName, whereTo);
    return this;
  }

  public TestOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
    core.setMetricsOutput(metric, whereTo);
    return this;
  }
}
