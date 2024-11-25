package org.uni.potsdam.p1.actors.operators.groups;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.actors.operators.FSMOperator;
import org.uni.potsdam.p1.actors.operators.tools.Analyser;
import org.uni.potsdam.p1.actors.operators.tools.SourceCounter;
import org.uni.potsdam.p1.execution.Settings;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;

import java.util.Map;

/**
 * <p>
 * Operator group to be used in global {@link Settings}. Contains a {@link SourceCounter},
 * a global scoped {@link FSMOperator} and an {@link Analyser}, which work together to
 * implement and monitor an operator:
 * </p>
 * <ul>
 *   <li>
 *     SourceCounter: Forwards events to the operator and calculate the operator's average
 *     input rates.
 *   </li>
 *   <li>
 *     FSMOperator: Processes events, calculates the average processing times of the event
 *     patterns as well as the processing rates of the inputs and forwards new complex
 *     events downstream.
 *   </li>
 *   <li>
 *     Analyser: Gathers processing times and input rates of the SourceCounter and FSMOperator,
 *     calculates the total average processing time for the given measurements, evaluates
 *     the necessity for load shedding and, if needed, contacts the {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}
 *     of this system, so that it can calculate and deploy the new load shedding rates for
 *     the FSMOperator.
 *   </li>
 * </ul>
 */
public class GlobalOperatorGroup extends AbstractOperatorGroup {
  public final MetricsOutput toThisAnalyser;
  public final FSMOperator operator;
  public final Analyser analyser;
  public final SourceCounter counter;
  public MetricsOutput toCoordinator;

  public SingleOutputStreamOperator<Measurement> counterDataStream;
  public SingleOutputStreamOperator<Metrics> analyserStream;

  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public GlobalOperatorGroup(OperatorInfo operatorInfo) {
    super(operatorInfo);
    toThisAnalyser = new MetricsOutput("out_to_analyser_" + operatorInfo.name);

    counter = new SourceCounter(operatorInfo)
      .setMetricsOutput("lambdaIn", toThisAnalyser)
      .withLogging("uff");

    operator = new FSMOperator(operatorInfo)
      .setMetricsOutput("ptime", toThisAnalyser);

    analyser = new Analyser(operatorInfo);

  }

  public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
    for (EventPattern pattern : operatorInfo.patterns) {
      for (String operatorName : pattern.downstreamOperators) {
        MeasurementOutput current = operatorGroupMap.get(operatorName).toThisOperator;
        operator.setSideOutput(operatorName, current);
      }
    }
  }

  /**
   * Connects this operator-group to the {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}
   * of the system.
   *
   * @param toCoordinator Connecting OutputTag.
   */
  public void connectToCoordinator(MetricsOutput toCoordinator) {
    this.toCoordinator = toCoordinator;
    counter.setMetricsOutput("sos", toCoordinator);
    operator.setMetricsOutput("sos", toCoordinator);
  }

  /**
   * Initialises the data stream connecting the input datastream of this operator with the
   * {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}'s message stream (global).
   * Initialises also the inner data streams of the operator-group, connecting its {@link SourceCounter},
   * {@link FSMOperator} and {@link Analyser} with each other and exposing the operator's
   * output stream to the system.
   *
   * @param global Message stream channel of the {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}
   */
  public void createDataStream(DataStream<String> global) {
    String opName = operatorInfo.name;

    counterDataStream =
      Settings.simpleConnect(inputDataStreams, global)
        .process(counter)
        .slotSharingGroup(opName)
        .name("Counter_" + opName);

    outputDataStream = Settings.simpleConnect(counterDataStream, global)
      .process(operator)
      .slotSharingGroup(opName)
      .name("Operator_" + opName);

    analyserStream = counterDataStream.getSideOutput(toThisAnalyser)
      .union(outputDataStream.getSideOutput(toThisAnalyser))
      .keyBy(map -> map.get("batch"))
      .process(analyser)
      .slotSharingGroup(opName)
      .name("Analyser_" + opName);

  }

  /**
   * Connects the {@link Metrics} outputs of this operator-group and links the merged datastream
   * with the global datastream connecting to the {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}.
   *
   * @param streamToCoordinator Global stream connecting all operator groups to the coordinator.
   * @return The updated global stream.
   */
  public DataStream<Metrics> gatherMetrics(DataStream<Metrics> streamToCoordinator) {
    DataStream<Metrics> metricsDataStream = analyserStream.union(counterDataStream.getSideOutput(toCoordinator)).union(outputDataStream.getSideOutput(toCoordinator));
    if (streamToCoordinator == null) {
      streamToCoordinator = metricsDataStream;
    } else {
      streamToCoordinator = streamToCoordinator.union(metricsDataStream);
    }
    return streamToCoordinator;
  }
}
