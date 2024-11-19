package org.uni.potsdam.p1.types;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.uni.potsdam.p1.types.outputTags.MeasurementOutput;
import org.uni.potsdam.p1.types.outputTags.MetricsOutput;
import org.uni.potsdam.p1.types.outputTags.StringOutput;


/**
 * <p>
 * This class stores the basic information of a DataStreamJob and serves as a standard
 * for implementing new Flink-Queries. Define here the following global parameters:
 * </p>
 * <ol>
 *   <li>Amount of records to be produced: BATCH_SIZE</li>
 *   <li>Amount of events used for calculating running averages at the OPERATORS:
 *   CONTROL_BATCH_SIZE</li>
 *   <li>Latency bound to be considered for the average processing time</li>
 *   <li>Function used to generate new records: EVENT_GENERATOR</li>
 *   <li>List containing all operators' information: OPERATORS</li>
 *   <li>Address of the kafka server if available: KAFKA_ADDRESS</li>
 *   <li>Additional kafka connect configurations</li>
 * </ol>
 */
public abstract class Settings {

  // GENERAL JOB INFORMATION
  public static final int RECORDS_PER_SECOND = 100;
  public static final int CONTROL_BATCH_SIZE = 100;
  public static final int BATCH_SIZE = 1_000_0;
  public static final double LATENCY_BOUND = 0.0001;
  public static final int TIME_WINDOW = 10;
  public static final GeneratorFunction<Long, Measurement> EVENT_GENERATOR = index -> new Measurement();

  // define output tags
  public final MetricsOutput toCoordinator = new MetricsOutput("out_to_coordinator");
  public final StringOutput toKafka = new StringOutput("out_to_kafka");
  public final MetricsOutput toAnalyser1 = new MetricsOutput("out_to_analyser1");
  public final MetricsOutput toAnalyser2 = new MetricsOutput("out_to_analyser2");
  public final MetricsOutput toAnalyser3 = new MetricsOutput("out_to_analyser3");
  public final MetricsOutput toAnalyser4 = new MetricsOutput("out_to_analyser4");
  public final MeasurementOutput toOperator4 = new MeasurementOutput("out_to_operator4");

  // define operators
  public OperatorInfo[] OPERATORS = new OperatorInfo[]{
    new OperatorInfo().withName("o1")
      .withInputTypes("1", "2", "3", "0")
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
      EventPattern.SEQ("11", "0|2:1|1", TIME_WINDOW, "o3"),
      EventPattern.AND("12", "1:2:3", TIME_WINDOW, "o4")
    ),

    new OperatorInfo().withName("o2")
      .withInputTypes("1", "2", "3", "0")
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
      EventPattern.SEQ("21", "0|2:1|1", TIME_WINDOW, "o3"),
      EventPattern.AND("22", "1:2:3", TIME_WINDOW, "o4")
    ),

    new OperatorInfo().withName("o3")
      .withInputTypes("11", "21")
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
        EventPattern.AND("1000", "11:21", 10, (String[]) null)
      ).toSink(),

    new OperatorInfo().withName("o4")
      .withInputTypes("12", "22")
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
        EventPattern.AND("2000", "12:22", 10, (String[]) null)
      ).toSink()
  };

  // define kafka's connection in the docker network
  public static final String KAFKA_ADDRESS = "kafka:9092";

  /*
    Set Kafka channels:
      -> globalChannelIn:   reads from kafka topic (global) containing the sos messages and
                            directs them to the OPERATORS
      -> globalChannelOut:  writes sos messages to kafka topic (global)
      -> control:           used for debugging and to gather metrics/outputs
   */
  public KafkaSource<String> globalChannelIn = KafkaSource.<String>builder()
    .setBootstrapServers(KAFKA_ADDRESS)
    .setTopics("global")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

  public KafkaSink<String> globalChannelOut = KafkaSink.<String>builder()
    .setBootstrapServers(KAFKA_ADDRESS)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("global")
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .build();

  public KafkaSink<String> control = KafkaSink.<String>builder()
    .setBootstrapServers(KAFKA_ADDRESS)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("globalOut")
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()
    )
    .build();

  /**
   * Execute the complex event detection job and return the system's results once finished.
   *
   * @return The execution results of the flink cluster
   * @throws Exception
   */
  public abstract JobExecutionResult execute() throws Exception;

  /**
   * Creates new Measurement records to be used in the DataStreamJob. Uses the event
   * generating function and specified amount of records defined in the fields
   * EVENT_GENERATOR and BATCH_SIZE of the extended {@link Settings} class.
   *
   * @param recordsPerSecond Maximum amount of records to be produced per second
   * @return A Flink DataGeneratorSource with the specified parameters.
   */
  public static DataGeneratorSource<Measurement> createMeasurementSource(int recordsPerSecond) {
    return new DataGeneratorSource<>(
      EVENT_GENERATOR,
      BATCH_SIZE,
      RateLimiterStrategy.perSecond(recordsPerSecond),
      TypeInformation.of(Measurement.class));
  }

  /**
   * Connects two data streams together using a constant key to bind their events. All
   * events are processed in the same keyed context.
   *
   * @param stream1 The first data stream
   * @param stream2 The second data stream
   * @param <T>     Object type of the events in the first data stream
   * @param <R>     Object type of the events in the second data stream
   * @return A connected stream {@link ConnectedStreams}
   */
  public static <T, R> ConnectedStreams<T, R> simpleConnect(DataStream<T> stream1, DataStream<R> stream2) {
    return stream1.keyBy(me -> 1).connect(stream2);
  }
}
