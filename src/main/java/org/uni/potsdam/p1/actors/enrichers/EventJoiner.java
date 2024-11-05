package org.uni.potsdam.p1.actors.enrichers;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Metrics;

import java.util.Map;

/**
 * This class is used to join the output rates' information from two operators if they
 * have multiple patterns that are used exclusively in many different operators. It gathers
 * this information and forward it to the respective operators' analysers.
 */
public class EventJoiner extends KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics> {
  MapState<String, Double> mapState;
  String[] keys;
  String[] keys2;
  OutputTag<Metrics> secondOutput;

  public EventJoiner(String[] keys, String[] keys2, OutputTag<Metrics> secondOutput) {
    this.keys = keys;
    this.keys2 = keys2;
    this.secondOutput = secondOutput;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapstate", String.class, Double.class));
  }

  @Override
  public void processElement1(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    processJob(mapState, value, keys, out, secondOutput, ctx);
  }

  @Override
  public void processElement2(Metrics value, KeyedCoProcessFunction<Double, Metrics, Metrics, Metrics>.Context ctx, Collector<Metrics> out) throws Exception {
    processJob(mapState, value, keys2, out, secondOutput, ctx);
  }

  /**
   * Creates new metrics for the operators downstream and fill them with their respective
   * relevant input information.
   *
   * @param mapState     State containing the output rates from one of the upstream operators
   * @param value        Current Metrics value acquired by the operator
   * @param keys         Event input types
   * @param out          Data collector in Flink
   * @param secondOutput Side output for second downstream operator
   * @param ctx          Flink's context variable
   * @throws Exception Flink's exceptions
   */
  public static void processJob(MapState<String, Double> mapState, Metrics value, String[] keys, Collector<Metrics> out, OutputTag<Metrics> secondOutput, Context ctx) throws Exception {
    if (!mapState.isEmpty()) {
      Metrics result = new Metrics("o3", "lambda", 4);
      Metrics result2 = new Metrics("o4", "lambda", 4);
      fillMap(result, mapState, value, new String[]{keys[0], keys[1]});
      fillMap(result2, mapState, value, new String[]{keys[2], keys[3]});
      out.collect(result);
      ctx.output(secondOutput, result2);
      mapState.clear();
    } else {
      for (Map.Entry<String, Double> entry : value.entrySet()) {
        mapState.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Fill metrics with the appropriate values.
   *
   * @param result   Updated metric instance
   * @param mapState Metrics kept in state
   * @param value    Current metrics acquired
   * @param keys1    Input types
   * @throws Exception Flink's exceptions
   */
  private static void fillMap(Metrics result, MapState<String, Double> mapState, Metrics value, String[] keys1) throws Exception {
    double first = value.get(keys1[0]);
    double second = mapState.get(keys1[1]);
    result.put("batch", value.get("batch"));
    result.put(keys1[0], first);
    result.put(keys1[1], second);
    result.put("total", first + second);
  }
}
