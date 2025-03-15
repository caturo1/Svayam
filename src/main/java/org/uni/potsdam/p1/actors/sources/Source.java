package org.uni.potsdam.p1.actors.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.uni.potsdam.p1.types.Event;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * <p>This class stores the basic information about a source to be used in an
 * {@link org.uni.potsdam.p1.execution.OperatorGraph}. Instances of this class should
 * be defined in the {@link org.uni.potsdam.p1.execution.Settings} of this project, where
 * they are used to parse the flink-job to be executed.
 * This class holds the following information about a source:</p>
 * <ul>
 *   <li>Name: the identifying name of a source (should be exclusive in the operator graph)</li>
 *   <li>Event Generator Function: describing how {@link Event} events are produced</li>
 *   <li>Batch size: amount of events produced</li>
 *   <li>Source stream: stream of events generated</li>
 *   <li>Downstream operators: all operator to which this source sends records to</li>
 * </ul>
 */
public class Source {
  public String name;
  public GeneratorFunction<Long, Event> eventGenerator;
  public int batchSize;
  public DataStream<Event> sourceStream;
  public String[] downstreamOperators;
  public String[] arg;
  public int recordsPerSecond = 0;

  public double mean;

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
    this.eventGenerator = index -> new Event(lowerBound, upperBound);
    return this;
  }

  /**
   * Define source's output types for a given selection (array/varargs).
   *
   * @param events Different event types to be produced.
   * @return Reference to this source.
   */
  public Source withOutputTypes(String... events) {
    this.eventGenerator = index -> new Event(events);
    return this;
  }

  /**
   * Sets the generator function for this source explicitly.
   * @param function The new generator function.
   * @return Reference to this source.
   */
  public Source withGeneratorFunction(FromFile function) {
    eventGenerator = function.use(arg);
    return this;
  }

  /**
   * Constructs a source that reads event types from a file and generates new measurements
   * in accordance. In order for this method to work the file should contain a single
   * integer per line (representing the event type).
   * @param pathToFile Path to the file containing the event types (should be present in
   *                   the machine executing the flink jobmanager)
   */
  public Source withDataFrom(Path pathToFile) {
    if (!Files.exists(pathToFile)) {
      throw new IllegalArgumentException("Given path is invalid. File does not exist.");
    }
    if (!Files.isReadable(pathToFile)) {
      throw new IllegalArgumentException("Given path is invalid. File is not readable.");
    }
    try (Stream<String> in = Files.lines(pathToFile)) {
      if(pathToFile.toString().endsWith(".csv")){
        arg = in.skip(1).toArray(String[]::new);
      } else {
        arg = in.toArray(String[]::new);
      }
      return this;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
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
   * Generates a stream of new events according to a Poisson distribution for every Event
   * generated by this source. Users are advised to set the attribute recordsPerSecond to 1
   * when using this mode.
   *
   * @param poissonMean The average amount of events to be generated.
   * @return Reference to this source.
   */
  public Source withPoissonDistribution(double poissonMean) {
    mean = poissonMean;
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
    this.eventGenerator = index -> new Event(lowerBound, upperBound);
    this.batchSize = batchSize;
  }

  /**
   * Constructs a source directly using the given parameters.
   *
   * @param name      Name of this source.
   * @param batchSize Amount of events to be produced.
   * @param events    Different event types to be produced.
   */
  public Source(String name, int batchSize, String... events) {
    this.name = name;
    this.eventGenerator = index -> new Event(events);
    this.batchSize = batchSize;
  }

  /**
   * Creates a {@link DataGeneratorSource} which will produce events in the flink environment.
   *
   * @return Event source for flink.
   */
  public DataGeneratorSource<Event> createMeasurementSource() {
    return new DataGeneratorSource<>(
      eventGenerator,
      batchSize,
      RateLimiterStrategy.perSecond(this.recordsPerSecond),
      TypeInformation.of(Event.class));
  }

  /**
   * Creates a {@link DataStream} to be used in a flink-{@link StreamExecutionEnvironment}
   * to direct this source's outputs to the operators.
   *
   * @param env The flink's stream execution environment
   */
  public void createDataStream(StreamExecutionEnvironment env) {
    if (sourceStream == null) {
      sourceStream =
        env.fromSource(createMeasurementSource(),
            WatermarkStrategy.noWatermarks(), name)
          .name(name);
    }
    if (mean > 0) {
      sourceStream = sourceStream.flatMap(new PoissonDataSource(mean));
//        .slotSharingGroup(executionGroup);
    }
  }
}

