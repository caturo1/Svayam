package org.uni.potsdam.p1.actors.operators.groups;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.actors.operators.FSMOperator;
import org.uni.potsdam.p1.actors.operators.tools.Analyser;
import org.uni.potsdam.p1.actors.operators.tools.SourceCounter;
import org.uni.potsdam.p1.execution.Settings;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

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
  public KafkaSink<String> globalChannelOut;
  public final StringOutput fromMessenger;

  public SingleOutputStreamOperator<Event> counterDataStream;
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
      .setMetricsOutput("lambdaIn", toThisAnalyser);

    operator = new FSMOperator(operatorInfo)
      .setMetricsOutput("ptime", toThisAnalyser);

    analyser = new Analyser(operatorInfo);
    fromMessenger = new StringOutput("from_m_to_" + operatorInfo.name);
  }

  public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
    for (EventPattern pattern : operatorInfo.patterns) {
      for (String operatorName : pattern.downstreamOperators) {
        EventOutput current = operatorGroupMap.get(operatorName).toThisOperator;
        operator.setSideOutput(operatorName, current);
      }
    }
  }

  public GlobalOperatorGroup connectToKafka(StringOutput toKafka,KafkaSink<String> globalChannelOut) {
    this.analyser.toKafka = toKafka;
    this.globalChannelOut = globalChannelOut;
    return this;
  }
  /**
   * Connects this operator-group to the {@link org.uni.potsdam.p1.actors.operators.tools.Coordinator}
   * of the system.
   *
   * @param toCoordinator Connecting OutputTag.
   */
  public GlobalOperatorGroup connectToCoordinator(MetricsOutput toCoordinator) {
    this.toCoordinator = toCoordinator;
    counter.setMetricsOutput("sos", toCoordinator);
    operator.setMetricsOutput("sos", toCoordinator);
    return this;
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
  public void createDataStream(SingleOutputStreamOperator<String> global) {
    String opName = operatorInfo.name;
    String executionGroup = operatorInfo.executionGroup;

    counterDataStream =
      inputDataStreams.connect(global)
        .process(counter)
        .name("Counter_" + opName);

    outputDataStream = counterDataStream.connect(global.union(global.getSideOutput(fromMessenger)))
      .process(operator)
      .name("Operator_" + opName);

    analyserStream = counterDataStream.getSideOutput(toThisAnalyser)
      .union(outputDataStream.getSideOutput(toThisAnalyser))
      .process(analyser)
      .name("Analyser_" + opName);

    if(executionGroup!=null) {
      counterDataStream.slotSharingGroup(executionGroup);
      outputDataStream.slotSharingGroup(executionGroup);
      analyserStream.slotSharingGroup(executionGroup);
    }

    analyserStream.getSideOutput(analyser.toKafka).sinkTo(globalChannelOut);
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
