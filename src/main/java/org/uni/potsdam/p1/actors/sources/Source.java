package org.uni.potsdam.p1.actors.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.uni.potsdam.p1.types.Measurement;

/**
 * <p>This class stores the basic information about a source to be used in an
 * {@link org.uni.potsdam.p1.execution.OperatorGraph}. Instances of this class should
 * be defined in the {@link org.uni.potsdam.p1.execution.Settings} of this project, where
 * they are used to parse the flink-job to be executed.
 * This class holds the following information about a source:</p>
 * <ul>
 *   <li>Name: the identifying name of a source (should be exclusive in the operator graph)</li>
 *   <li>Event Generator Function: describing how {@link Measurement} events are produced</li>
 *   <li>Batch size: amount of events produced</li>
 *   <li>Source stream: stream of events generated</li>
 *   <li>Downstream operators: all operator to which this source sends records to</li>
 * </ul>
 */
public class Source {
  public String name;
  public GeneratorFunction<Long, Measurement> eventGenerator;
  public int batchSize;
  public DataStream<Measurement> sourceStream;
  public String[] downstreamOperators;
  public int recordsPerSecond = 0;

  /**
   * Constructs new source
   */
  public Source() {
  }

  /**
   * Define source's name.
   *
   * @param name Name to be given.
   * @return Reference to this source.
   */
  public Source withName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Define source's batch size.
   *
   * @param batchSize Amount of events to be produced.
   * @return Reference to this source.
   */
  public Source withBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  /**
   * Define source's output types in a given range.
   *
   * @param lowerBound Smallest type to be produced.
   * @param upperBound Exclusive upper bound for event types.
   * @return Reference to this source.
   */
  public Source withOutputTypes(int lowerBound, int upperBound) {
    this.eventGenerator = index -> new Measurement(lowerBound, upperBound);
    return this;
  }

  /**
   * Define source's output types for a given selection (array/varargs).
   *
   * @param events Different event types to be produced.
   * @return Reference to this source.
   */
  public Source withOutputTypes(int... events) {
    this.eventGenerator = index -> new Measurement(events);
    return this;
  }

  /**
   * Define source's downstream operators.
   *
   * @param operators All operators to receive this source's outputs.
   * @return Reference to this source.
   */
  public Source withDownStreamOperators(String... operators) {
    this.downstreamOperators = operators;
    return this;
  }

  /**
   * Define amount of events to be produced per second.
   *
   * @param recordsPerSecond Amount of events to be produced in a second.
   * @return Reference to this source.
   */
  public Source withRecordsPerSecond(int recordsPerSecond) {
    this.recordsPerSecond = recordsPerSecond;
    return this;
  }

  /**
   * Constructs a source directly using the given parameters.
   *
   * @param name       Name of this source.
   * @param batchSize  Amount of events to be produced.
   * @param lowerBound Smallest type to be produced.
   * @param upperBound Exclusive upper bound for event types.
   */
  public Source(String name, int batchSize, int lowerBound, int upperBound) {
    this.name = name;
    this.eventGenerator = index -> new Measurement(lowerBound, upperBound);
    this.batchSize = batchSize;
  }

  /**
   * Constructs a source directly using the given parameters.
   *
   * @param name      Name of this source.
   * @param batchSize Amount of events to be produced.
   * @param events    Different event types to be produced.
   */
  public Source(String name, int batchSize, int... events) {
    this.name = name;
    this.eventGenerator = index -> new Measurement(events);
    this.batchSize = batchSize;
  }

  /**
   * Creates a {@link DataGeneratorSource} which will produce events in the flink environment.
   *
   * @return Event source for flink.
   */
  public DataGeneratorSource<Measurement> createMeasurementSource() {
    return new DataGeneratorSource<>(
      eventGenerator,
      batchSize,
      RateLimiterStrategy.perSecond(this.recordsPerSecond),
      TypeInformation.of(Measurement.class));
  }

  /**
   * Creates a {@link DataStream} to be used in a flink-{@link StreamExecutionEnvironment}
   * to direct this source's outputs to the operators.
   *
   * @param env The flink's stream execution environment
   */
  public void createDataStream(StreamExecutionEnvironment env) {
    if (sourceStream == null) {
      sourceStream = env.fromSource(createMeasurementSource(),
        WatermarkStrategy.noWatermarks(), name).name(name);
    }
  }
}
