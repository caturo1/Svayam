package org.uni.potsdam.p1.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.actors.operators.groups.*;
import org.uni.potsdam.p1.actors.operators.tools.Coordinator;
import org.uni.potsdam.p1.actors.operators.tools.Messenger;
import org.uni.potsdam.p1.actors.sources.Source;
import org.uni.potsdam.p1.heuristic.HybridMessenger;
import org.uni.potsdam.p1.heuristic.HybridOperatorGroup;
import org.uni.potsdam.p1.hybrid.Hybrid2;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.Metrics;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.Scope;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;
import org.uni.potsdam.p1.variant.Variant1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * This class parses and executes the Flink-Query to be executed with global or local load
 * shedding properties in accordance to attributes defined in {@link Settings2Layer}.
 * It creates new sources ({@link Source}) and operator groups ({@link AbstractOperatorGroup})
 * using the information provided in the settings, connects them and prepares an
 * executable Flink-query which can be used in the {@link org.uni.potsdam.p1.DataStreamJob}'s
 * main method to deploy the job to a flink cluster.
 * </p>
 * <p>
 * If the setting GLOBAL_SCOPE is set to true in {@link Settings2Layer}, then this class assumes
 * that the flink cluster is being executed in a docker network, in which a kafka server
 * is also reachable at kafka:9092. It assumes also that this kafka server contains the
 * topics global and globalOut. Failing to provide this will cause the application to fail.
 * </p>
 * <p>
 * We recommend using the flink-swarm.yml file to deploy a pre-defined docker network, which
 * contains kafka and flink and deploy this application there.
 * </p>
 */
public class OperatorGraph extends Settings3Layer {
  private static final Logger oLog = LoggerFactory.getLogger(OperatorGraph.class);
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
  public KafkaSink<String> producerChannel;
  public KafkaSource<String> globalChannelIn;
  public KafkaSink<String> globalChannelOut;
  public StringOutput toKafka;


  SingleOutputStreamOperator<String> global = null;

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
    if (SCOPE == Scope.GLOBAL || SCOPE == Scope.VARIANT) {

      KAFKA_ADDRESS = "kafka:9092";
      toKafka = new StringOutput("out_to_kafka");
      globalChannelIn =
        KafkaSource.<String>builder()
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

      coordinator = new Coordinator(toKafka, LATENCY_BOUND, operators);
      toCoordinator = new MetricsOutput("out_to_coordinator");

    }

    if (SCOPE == Scope.HYBRID) {
      KAFKA_ADDRESS = "kafka:9092";
      toKafka = new StringOutput("out_to_kafka");

      List<String> requiredTopics = new ArrayList<>();
      for (OperatorInfo operator : operators) {
        requiredTopics.add(operator.name);
      }

      KafkaTopicManager.ensureTopicExists(requiredTopics, KAFKA_ADDRESS);
      producerChannel = KafkaSink.<String>builder()
        .setBootstrapServers(KAFKA_ADDRESS)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                // Replace your current topic selector with this for better debugging
                .setTopicSelector(element -> {
                  String msg = (String) element;
                  //oLog.info("Topic selection for message of length: " + msg.length());
                  
                  try {
                      // Always log the first part of the message for debugging
                      String firstPart = msg.length() > 50 ? msg.substring(0, 50) + "..." : msg;
                      //oLog.info("Message content: " + msg);
                    
                      String targetPattern = "target:([^\\s]+)";
                      Pattern pattern = Pattern.compile(targetPattern);
                      Matcher matcher = pattern.matcher(msg);
                      
                      if (matcher.find()) {
                          String topic = matcher.group(1);
                          //oLog.info("Extracted topic: '" + topic + "'");
                          return topic;
                      } else {
                        return "default";
                      }

                  } catch (Exception e) {
                      oLog.error("No target found in message: " + e.getMessage(), e + " -- using default");
                      throw new RuntimeException("Topic selector not able to redirect message");
                  }
                })
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
        )
        .build();
    }


    this.operators = new HashMap<>(operators.length);
    for (OperatorInfo operator : operators) {
      AbstractOperatorGroup newGroup;
      if (SCOPE == Scope.GLOBAL) {
        newGroup = new GlobalOperatorGroup(operator);
        ((GlobalOperatorGroup) newGroup)
          .connectToCoordinator(toCoordinator)
          .connectToKafka(toKafka, globalChannelOut);
      } else if (SCOPE == Scope.LOCAL) {
        newGroup = new LocalOperatorGroup(operator);
      } else if (SCOPE == Scope.VARIANT) {
        newGroup = new Variant1(operator)
          .connectToCoordinator(toCoordinator)
          .connectToKafka(toKafka, globalChannelOut);
      } else if (SCOPE == Scope.HYBRID) {
        newGroup = new HybridOperatorGroup(operator)
                .connectToKafka(toKafka, producerChannel);
      } else {
        newGroup = new BasicOperatorGroup(operator);
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
    
    for (Source source : sources.values()) {
      source.createDataStream(env);
      for (String opDownStream : source.downstreamOperators) {
        operators.get(opDownStream).addInputStream(source.sourceStream);
      }
    }
    
    if (SCOPE == Scope.GLOBAL || SCOPE == Scope.VARIANT) {
      // define a keyed data stream with which the operators send their information to the coordinator
      global = env.fromSource(globalChannelIn, WatermarkStrategy.noWatermarks(), "global")
        .process(new Messenger(operators, SCOPE))
//        .slotSharingGroup("Messenger")
        .name("Messenger");
    }

    Map<String, DataStream<String>> kafkaInputs = new HashMap<>();
    if (SCOPE == Scope.HYBRID) {
      // Create Kafka consumers for each operator once
      for (OperatorInfo operator : OPERATORS) {
        String opName = operator.name;
        
        KafkaSource<String> opConsumer = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_ADDRESS)
                .setTopics(opName)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaInput = env.fromSource(opConsumer, 
                                                    WatermarkStrategy.noWatermarks(), 
                                                    "opConsumer_" + opName);
        kafkaInputs.put(opName, kafkaInput);
      }
    }



    for (OperatorInfo operator : OPERATORS) {
      AbstractOperatorGroup current = operators.get(operator.name);
      if (SCOPE == Scope.GLOBAL) {
        ((GlobalOperatorGroup) current).createDataStream(global);
        streamToCoordinator = ((GlobalOperatorGroup) current).gatherMetrics(streamToCoordinator);
      } else if (SCOPE == Scope.LOCAL) {
        ((LocalOperatorGroup) current).createDataStream();
      } else if (SCOPE == Scope.VARIANT) {
        ((Variant1) current).createDataStream(global);
        streamToCoordinator = ((Variant1) current).gatherMetrics(streamToCoordinator);
      } else if (SCOPE == Scope.HYBRID) {
        DataStream<String> kafkaInput = kafkaInputs.get(operator.name);
        if (kafkaInput != null) {
          ((HybridOperatorGroup) current).createDataStream(kafkaInput);
        } else {
          oLog.info("Kafka not properly set up");
          throw new IllegalArgumentException("Fuck me, kafka input broken boy");
        }
      } else {
        ((BasicOperatorGroup) current).createDataStream();
      }
      for (EventPattern pattern : current.operatorInfo.patterns) {
        for (String opDownStream : pattern.downstreamOperators) {
          operators.get(opDownStream).addInputStream(current.outputDataStream);
          if (SCOPE == Scope.VARIANT) {
            Variant1 downstreamOp = (Variant1) operators.get(opDownStream);
            downstreamOp.addAnalyserInputStream(current.outputDataStream);
          }
          if (SCOPE == Scope.HYBRID) {
            if (operators.get(opDownStream) instanceof HybridOperatorGroup &&
                current instanceof HybridOperatorGroup) {
              HybridOperatorGroup downstreamOp = (HybridOperatorGroup) operators.get(opDownStream);
              HybridOperatorGroup currentHybrid = (HybridOperatorGroup) current;
              
              // Now use the cast variables
              downstreamOp.addAnalyzerInputStream(currentHybrid.outputDataStream);
              if (currentHybrid.analyserOutputStream != null) {
                downstreamOp.addAnalyserSOSStream(currentHybrid.analyserOutputStream);
              }

            } else {
              oLog.info("Warning: Expected HybridOperatorGroup but got " + 
                                  operators.get(opDownStream).getClass().getName() + 
                                  " or " + current.getClass().getName());
            }
          }
        }
      }
    }

    if (SCOPE == Scope.GLOBAL || SCOPE == Scope.VARIANT) {

      // execute coordinator
      SingleOutputStreamOperator<String> coordinatorOutput = streamToCoordinator
        .process(coordinator)
//        .slotSharingGroup("Coordinator")
        .name("Coordinator");

      // store shedding rates in kafka
      coordinatorOutput.getSideOutput(toKafka).sinkTo(globalChannelOut);
    }

    return env.execute("Flink Java CEP Prototype");
  }
}
