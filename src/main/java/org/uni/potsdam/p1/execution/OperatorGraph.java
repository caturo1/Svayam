package org.uni.potsdam.p1.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.uni.potsdam.p1.actors.operators.groups.AbstractOperatorGroup;
import org.uni.potsdam.p1.actors.operators.groups.GlobalOperatorGroup;
import org.uni.potsdam.p1.actors.operators.groups.LocalOperatorGroup;
import org.uni.potsdam.p1.actors.operators.tools.Coordinator;
import org.uni.potsdam.p1.actors.sources.Source;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * This class parses and executes the Flink-Query to be executed with global or local load
 * shedding properties in accordance to attributes defined in {@link Settings}.
 * It creates new sources ({@link Source}) and operator groups ({@link AbstractOperatorGroup})
 * using the information provided in the settings, connects them and prepares an
 * executable Flink-query which can be used in the {@link org.uni.potsdam.p1.DataStreamJob}'s
 * main method to deploy the job to a flink cluster.
 * </p>
 * <p>
 * If the setting GLOBAL_SCOPE is set to true in {@link Settings}, then this class assumes
 * that the flink cluster is being executed in a docker network, in which a kafka server
 * is also reachable at kafka:9092. It assumes also that this kafka server contains the
 * topics global and globalOut. Failing to provide this will cause the application to fail.
 * </p>
 * <p>
 * We recommend using the flink-swarm.yml file to deploy a pre-defined docker network, which
 * contains kafka and flink and deploy this application there.
 * </p>
 */
public class OperatorGraph extends Settings {
  Map<String, Source> sources;
  Map<String, AbstractOperatorGroup> operators;
  Coordinator coordinator;
  MetricsOutput toCoordinator;
  DataStream<Metrics> streamToCoordinator;

  /*
    Set Kafka channels:
      -> globalChannelIn:   reads from kafka topic (global) containing the sos messages and
                            directs them to the OPERATORS
      -> globalChannelOut:  writes sos messages to kafka topic (global)
      -> control:           used for debugging and to gather metrics/outputs
   */
  public String KAFKA_ADDRESS;
  public KafkaSource<String> globalChannelIn;
  public KafkaSink<String> globalChannelOut;
  public KafkaSink<String> control;
  public StringOutput toKafka;

  /**
   * Constructs the operator graph in a local or global scope. Preparing its components
   * accordingly.
   */
  public OperatorGraph() {
    OperatorInfo[] operators = OPERATORS;
    Source[] sources = SOURCES;

    this.sources = new HashMap<>(sources.length);
    for (Source source : sources) {
      this.sources.put(source.name, source);
    }
    if (GLOBAL_SCOPE) {
      KAFKA_ADDRESS = "kafka:9092";
      toKafka = new StringOutput("out_to_kafka");
      globalChannelIn = KafkaSource.<String>builder()
        .setBootstrapServers(KAFKA_ADDRESS)
        .setTopics("global")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

      globalChannelOut = KafkaSink.<String>builder()
        .setBootstrapServers(KAFKA_ADDRESS)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
          .setTopic("global")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
        )
        .build();

      control = KafkaSink.<String>builder()
        .setBootstrapServers(KAFKA_ADDRESS)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
          .setTopic("globalOut")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
        )
        .build();
      coordinator = new Coordinator(toKafka, LATENCY_BOUND, operators);
      toCoordinator = new MetricsOutput("out_to_coordinator");
    }

    this.operators = new HashMap<>(operators.length);
    for (OperatorInfo operator : operators) {
      AbstractOperatorGroup newGroup;
      if (GLOBAL_SCOPE) {
        newGroup = new GlobalOperatorGroup(operator);
        ((GlobalOperatorGroup) newGroup).connectToCoordinator(toCoordinator);
      } else {
        newGroup = new LocalOperatorGroup(operator);
      }
      this.operators.put(operator.name, newGroup);
    }
    this.operators.values().forEach(operatorGroup -> operatorGroup.setOutputs(this.operators));
  }

  /**
   * Use the given environment-,operator- and source-information to create an executable
   * flink-query by connecting all the defined components appropriately.
   *
   * @return Results of execution of the flink-job.
   * @throws Exception Internal flink errors or NullPointerExceptions if the operators are
   *                   badly connected (not defined in order, input types do not match output types of previous
   *                   components).
   */
  @Override
  public JobExecutionResult execute() throws Exception {

    // start execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KeyedStream<String, Integer> global;
    if (GLOBAL_SCOPE) {
      // define a keyed data stream with which the operators send their information to the coordinator
      global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global").keyBy(st -> 1);
    }

    for (Source source : sources.values()) {
      source.createDataStream(env, RECORDS_PER_SECOND);
      for (String opDownStream : source.downstreamOperators) {
        operators.get(opDownStream).addInputStream(source.sourceStream);
      }
    }

    for (OperatorInfo operator : OPERATORS) {
      AbstractOperatorGroup current = operators.get(operator.name);
      if (GLOBAL_SCOPE) {
        ((GlobalOperatorGroup) current).createDataStream(global);
        streamToCoordinator = ((GlobalOperatorGroup) current).gatherMetrics(streamToCoordinator);
      } else {
        ((LocalOperatorGroup) current).createDataStream();
      }
      for (EventPattern pattern : current.operatorInfo.patterns) {
        for (String opDownStream : pattern.downstreamOperators) {
          operators.get(opDownStream).addInputStream(current.outputDataStream);
        }
      }
    }

    if (GLOBAL_SCOPE) {

      // execute coordinator
      SingleOutputStreamOperator<String> coordinatorOutput = streamToCoordinator
        .keyBy(metric -> metric.id).process(coordinator).name("Coordinator");

      // store shedding rates in kafka
      coordinatorOutput.getSideOutput(toKafka).sinkTo(globalChannelOut);

      // output stream for debugging - returns the results of the coordinator
      coordinatorOutput.sinkTo(control);
    }

    return env.execute("Flink Java CEP Prototype");
  }
}
