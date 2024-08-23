package org.uni.potsdam.p1.types;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.InputRate;

public class Shedder extends ProcessFunction<Measurement, Measurement> {

  //  ValueState<InputRate> shedRates;
  ValueState<InputRate> input;
  ValueState<Integer> sum;

  private final OutputTag<InputRate> output;

  public Shedder(OutputTag<InputRate> output) {
    this.output = output;
  }

  @Override
  public void open(OpenContext openContext) {
//    shedRates = getRuntimeContext().getState(new ValueStateDescriptor<>("shedRates",
//      InputRate.class));
    input = getRuntimeContext().getState(new ValueStateDescriptor<>("input",
      InputRate.class));
    sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum",
      Integer.class));
  }

  @Override
  public void processElement(Measurement value, Context ctx,
                             Collector<Measurement> out) throws Exception {
    if (input.value() == null) {
      sum.update(0);
      input.update(new InputRate());
      System.out.println(input);
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10);
    }
    input.value().countEvent(String.valueOf(value.machineId), 1);

    double prob = Math.random();

    if (input.value().isValid(String.valueOf(value.machineId), prob)) {
      out.collect(value);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Measurement> out) throws Exception {
    sum.update(sum.value() + input.value().getTotal());
    ctx.output(output, input.value());
    input.value().clear();
    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10);
  }
}
