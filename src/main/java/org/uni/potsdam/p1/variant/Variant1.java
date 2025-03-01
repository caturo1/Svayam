package org.uni.potsdam.p1.variant;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.actors.operators.groups.AbstractOperatorGroup;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

import java.util.Map;

public class Variant1 extends AbstractOperatorGroup {

  public final MetricsOutput toAnalyser;
  public final TestOperator operator;
  public final Variant1Analyser analyser;
  public MetricsOutput toCoordinator;
  public KafkaSink<String> globalChannelOut;
  public final StringOutput fromMessenger;
  public DataStream<Metrics> analyserInputDataStreams;

  public SingleOutputStreamOperator<Metrics> analyserStream;

  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public Variant1(OperatorInfo operatorInfo) {
    super(operatorInfo);

    toAnalyser = new MetricsOutput("out_to_analyser_" + operatorInfo.name);

    operator = new TestOperator(operatorInfo)
      .setMetricsOutput("ptime", toAnalyser);

    analyser = new Variant1Analyser(operatorInfo);

    fromMessenger = new StringOutput("from_m_to_" + operatorInfo.name);
  }

  @Override
  public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
    for (EventPattern pattern : operatorInfo.patterns) {
      for (String operatorName : pattern.downstreamOperators) {
        EventOutput downstreamOperator = operatorGroupMap.get(operatorName).toThisOperator;
        operator.setSideOutput(operatorName, downstreamOperator);
        Variant1 downstreamOp = (Variant1) operatorGroupMap.get(operatorName);
        operator.setMetricsOutput("lambdaOut",downstreamOp.toAnalyser);
      }
    }
  }

  public void addAnalyserInputStream(SingleOutputStreamOperator<Event> inputStream) {
    DataStream<Metrics> stream = inputStream.getSideOutput(toAnalyser);
    if (analyserInputDataStreams == null) {
      analyserInputDataStreams = stream;
    } else {
      analyserInputDataStreams = analyserInputDataStreams.union(stream);
    }
  }

  @Override
  public void addInputStream(DataStream<Event> inputStream) {
    if (inputDataStreams == null) {
      inputDataStreams = inputStream;
    } else {
      inputDataStreams = inputDataStreams.union(inputStream);
    }

    DataStream<Metrics> sourceOutputRates = inputStream
    .process(new VariantSourceCounter(operatorInfo))
//      .slotSharingGroup(operatorInfo.name)
      .name("Source Counter");
    if (analyserInputDataStreams == null) {
      analyserInputDataStreams = sourceOutputRates;
    } else {
      analyserInputDataStreams = analyserInputDataStreams.union(sourceOutputRates);
    }

  }

  public Variant1 connectToKafka(StringOutput toKafka,KafkaSink<String> globalChannelOut) {
    this.analyser.toKafka = toKafka;
    this.globalChannelOut = globalChannelOut;
    return this;
  }

  public Variant1 connectToCoordinator(MetricsOutput toCoordinator) {
    this.toCoordinator = toCoordinator;
    operator.setMetricsOutput("sos", toCoordinator);
    return this;
  }

  public DataStream<Metrics> gatherMetrics(DataStream<Metrics> streamToCoordinator) {
    DataStream<Metrics> metricsDataStream = analyserStream.union(outputDataStream.getSideOutput(toCoordinator));
    if (streamToCoordinator == null) {
      streamToCoordinator = metricsDataStream;
    } else {
      streamToCoordinator = streamToCoordinator.union(metricsDataStream);
    }
    return streamToCoordinator;
  }

  public void createDataStream(SingleOutputStreamOperator<String> global) {
    String opName = operatorInfo.name;
    String executionGroup = operatorInfo.executionGroup;

    outputDataStream = inputDataStreams.connect(global.union(global.getSideOutput(fromMessenger)))
      .process(operator)
      .name("Operator_" + opName);

    analyserStream = analyserInputDataStreams.union(outputDataStream.getSideOutput(toAnalyser))
      .connect(global)
      .process(analyser)
      .name("Analyser_" + opName);

    if(executionGroup!=null) {
      outputDataStream.slotSharingGroup(executionGroup);
      analyserStream.slotSharingGroup(executionGroup);
    }

    analyserStream.getSideOutput(analyser.toKafka).sinkTo(globalChannelOut);
  }


}
