package org.uni.potsdam.p1.types;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.InputRate;

public class EventCollector extends ProcessFunction<String, InputRate> {
  ValueState<InputRate> numb;

  @Override
  public void open(OpenContext openContext) {
    numb = getRuntimeContext().getState(new ValueStateDescriptor<>("numb",
      InputRate.class));
  }

  @Override
  public void processElement(String value, ProcessFunction<String, InputRate>.Context ctx,
                             Collector<InputRate> out) throws Exception {
    if (numb.value() == null) {
      numb.update(new InputRate());
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10);
    }
    numb.value().countEvent(value, 1);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<InputRate> out) throws Exception {
    out.collect(numb.value());
    numb.value().clear();
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10);
  }
}