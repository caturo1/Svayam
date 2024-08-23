package org.uni.potsdam.p1.types;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class PatternProcessor extends PatternProcessFunction<Measurement, ComplexEvent> {

  private final OutputTag<String> output;
  private final String name;

  public PatternProcessor(String name, OutputTag<String> output) {
    this.name = name;
    this.output = output;
  }

  @Override
  public void processMatch(Map<String, List<Measurement>> match, Context ctx,
                           Collector<ComplexEvent> out) {
    ctx.output(output, name);
    Measurement start = match.get("start").get(0);
    Measurement middle = match.get("middle").get(0);
    Measurement end = match.get("end").get(0);
    String result = name +
      "- D : " + start.eventTime + " " +
      middle.eventTime + " " +
      end.eventTime;
    out.collect(new ComplexEvent(result, ctx.timestamp()));
  }
}
