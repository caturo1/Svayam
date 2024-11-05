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

  public static final int CONTROL_BATCH_SIZE = 100;
  public static final int BATCH_SIZE = 10_000;
  public static double LATENCY_BOUND = 15.15E-3;
  public static final GeneratorFunction<Long, Measurement> EVENT_GENERATOR = Measurement::new;

  public String[] SOURCE_TYPES = new String[]{"0", "1", "2", "3"};
  public String[] O1_OUTPUT_TYPES = new String[]{"11", "12"};
  public String[] O2_OUTPUT_TYPES = new String[]{"21", "22"};
  public String[] O3_INPUT_TYPES = new String[]{"11", "21"};
  public String[] O4_INPUT_TYPES = new String[]{"12", "22"};
  public String[] O3_OUTPUT_TYPES = new String[]{"1000"};
  public String[] O4_OUTPUT_TYPES = new String[]{"2000"};
  public OperatorInfo[] OPERATORS = new OperatorInfo[]{
    new OperatorInfo().withName("o1")
      .withInputTypes("1", "2", "3", "0")
      .withPatterns(
      EventPattern.SEQ("11", "0|2:1|1", "o3"),
      EventPattern.AND("12", "1:2:3", "o4")
    ),

    new OperatorInfo().withName("o2")
      .withInputTypes("1", "2", "3", "0")
      .withPatterns(
      EventPattern.SEQ("21", "0|2:1|1", "o3"),
      EventPattern.AND("22", "1:2:3", "o4")
    ),

    new OperatorInfo().withName("o3")
      .withInputTypes("11", "21")
      .withPatterns(
        EventPattern.AND("1000", "11:21", (String[]) null)
      ).toSink(),

    new OperatorInfo().withName("o4")
      .withInputTypes("12", "22")
      .withPatterns(
        EventPattern.AND("2000", "12:22", (String[]) null)
      ).toSink()
  };

  public static final String KAFKA_ADDRESS = "kafka1:19092";
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
    .setStartingOffsets(OffsetsInitializer.earliest())
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

  public abstract JobExecutionResult execute() throws Exception;

  /**
   * Creates new Measurement records to be used in the DataStreamJob. Uses the event
   * generating function and specified amount of records defined in the fields
   * EVENT_GENERATOR and BATCH_SIZE of the extended {@link Settings} class.
   *
   * @param recordsPerSecond Maximum amount of records to be produced per second
   * @return A Flink DataGeneratorSource with the specified parameters.
   */
  public static DataGeneratorSource<Measurement> createMeasuremetSource(int recordsPerSecond) {
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
