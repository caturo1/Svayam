package org.uni.potsdam.p1.variant;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.actors.measurers.CountingMeasurer;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import java.util.Set;

public class VariantSourceCounter extends ProcessFunction<Event, Metrics> {

  // define the Measurers for the stream characteristics
  CountingMeasurer inputRateMeasurer;
  String name;
  public Set<String> typeChecker;

  public VariantSourceCounter(OperatorInfo operator) {
    inputRateMeasurer = new CountingMeasurer(operator.name, operator.inputTypes, "lambdaIn", operator.controlBatchSize);
    this.name = operator.name;
    typeChecker = operator.typeChecker;
  }
  @Override
  public void processElement(Event value, ProcessFunction<Event, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    if(typeChecker.contains(value.type)) {
      inputRateMeasurer.update(value.getTypeAsKey());
      if (inputRateMeasurer.isReady()) {
        out.collect(inputRateMeasurer.getNewestAverages());
      }
    }
  }
}
