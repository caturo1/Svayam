package org.uni.potsdam.p1.actors.operators;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.actors.measurers.Measurer;
import org.uni.potsdam.p1.actors.operators.cores.GlobalOperatorCore;
import org.uni.potsdam.p1.actors.operators.tools.Coordinator;
import org.uni.potsdam.p1.actors.processors.FSMProcessor;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

/**
 * <p>
 * This class serves as the basic operator standard for processing complex events.
 * A FSMOperator can detect multiple event patterns and have multiple outputs. It
 * processes events using a {@link FSMProcessor} for each pattern and records basic
 * {@link Metrics} about the quality of its pattern-detection-process like:
 * </p>
 * <ul>
 *     <li>output rates = lambdaOut</li>
 *     <li>processing rates = mu</li>
 *     <li>processing time pro parameters = ptime</li>
 * </ul>
 * <p>
 * Instances of this class should receive a connected keyed stream of Measurements and Strings.
 * The Measurement-stream for the events to be processed and the String-stream for
 * Coordinator-messages.
 * </p>
 *
 * @see <a href=https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#keyed-streams">https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#keyed-streams</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#connected-streams">https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/etl/#connected-streams</a>
 * @see Coordinator
 */
public class FSMOperator extends KeyedCoProcessFunction<Long, Measurement, String, Measurement> {

  GlobalOperatorCore core;

  /**
   * Constructs a new processor using the given operator's information as well as the
   * specified batchSize for the {@link Measurer} instances used to calculate the processing
   * {@link Metrics}.
   *
   * @param operator Information about this operator.
   */
  public FSMOperator(OperatorInfo operator) {

    core = new GlobalOperatorCore(operator);

  }

  /**
   * Set a new side output for a downstream operator to be reached
   *
   * @param operatorName Name/Type of the operators to be referenced.
   * @param whereTo      Output channel where the events are to be forwarded to
   * @return A reference to this instance.
   */
  public FSMOperator setSideOutput(String operatorName, MeasurementOutput whereTo) {
    core.setSideOutput(operatorName, whereTo);
    return this;
  }

  //
//  /**
//   * Set a new side output for one metric calculated by this operator
//   *
//   * @param metric  Name of the metric to be referenced
//   * @param whereTo Output channel where the metrics are to be forwarded to
//   * @return A reference to this instance.
//   */
  public FSMOperator setMetricsOutput(String metric, MetricsOutput whereTo) {
    core.setMetricsOutput(metric, whereTo);
    return this;
  }

  /**
   * Processes each Measurement event in all patterns of this operator, for which the
   * pattern-specific shedding rate is greater than a pseudo-random value. Measures and
   * updates the processing time as well as the processing and output rates.
   *
   * @param value The stream element
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements (not utilized)
   * @throws Exception Error in the flink's thread execution.
   */
  @Override
  public void processElement1(Measurement value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {

    core.processWithContext(value, ctx);

  }

  /**
   * Sends the newest calculated metrics to coordinator if it receives a sos-message (snap)
   * or updates its own shedding rates if the message receive contains this operator's name.
   * Informs this operator's analyser that shedding has been activated/deactivated.
   *
   * @param value The stream element
   * @param ctx   A {@link Context} that allows querying the timestamp of the element, querying the
   *              TimeDomain of the firing timer and getting a TimerService for registering
   *              timers and querying the time. The context is only valid during the invocation of this
   *              method, do not store it.
   * @param out   The collector to emit resulting elements to
   * @throws Exception Flink's error
   */
  @Override
  public void processElement2(String value, KeyedCoProcessFunction<Long, Measurement, String, Measurement>.Context ctx, Collector<Measurement> out) throws Exception {
    core.processMessages(value, ctx);
  }
}
