package org.uni.potsdam.p1.actors.jobs;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.uni.potsdam.p1.actors.operators.LocalOperator;
import org.uni.potsdam.p1.actors.operators.Logger;
import org.uni.potsdam.p1.types.Measurement;
import org.uni.potsdam.p1.types.Settings;

/**
 * This class holds the Flink-Query to be executed with local load shedding properties.
 * Here we define sources, operators and sinks as well as the order in which the data
 * is processed. Define the characteristics of your {@link StreamExecutionEnvironment}
 * in the {@link Settings} class and execute your job by initialising it and calling
 * {@link GlobalJob#execute()} in the main method of {@link org.uni.potsdam.p1.DataStreamJob}.
 * This class serves as an implementation standard, users should define their application's
 * jobs in their own implementations of {@link Settings}.
 */
public class LocalJob extends Settings {

  /**
   * Defines the Flink query to be executed.
   *
   * @return Results of this job's execution
   * @throws Exception Error's in the execution of the flink-thread.
   */
  @Override
  public JobExecutionResult execute() throws Exception {

    // start execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // define the data sources for the job
    DataStream<Measurement> source1 = env.fromSource(
        createMeasurementSource(RECORDS_PER_SECOND),
        WatermarkStrategy.noWatermarks(), "Generator1")
      .map(new Logger("s1"))
      .slotSharingGroup("s1")
      .name("Source1");

    DataStream<Measurement> source2 = env.fromSource(
        createMeasurementSource(RECORDS_PER_SECOND),
        WatermarkStrategy.noWatermarks(), "Generator2")
      .map(new Logger("s2"))
      .slotSharingGroup("s2")
      .name("Source2");

    // set OPERATORS 1 and 2
    SingleOutputStreamOperator<Measurement> operator1 =
      source1
        .process(new LocalOperator(OPERATORS[0])
          .setSideOutput("12", toOperator4))
        .slotSharingGroup("o1")
        .name("Operator1");

    SingleOutputStreamOperator<Measurement> operator2 =
      source2
        .process(new LocalOperator(OPERATORS[1])
          .setSideOutput("22", toOperator4))
        .slotSharingGroup("o2")
        .name("Operator2");

    operator1
      .union(operator2)
      .process(new LocalOperator(OPERATORS[2]))
      .slotSharingGroup("o3")
      .name("Operator3");

    operator1.getSideOutput(toOperator4)
      .union(operator2.getSideOutput(toOperator4))
      .process(new LocalOperator(OPERATORS[3]))
      .slotSharingGroup("o4")
      .name("Operator4");

    return env.execute("Flink Java CEP Prototype");
  }
}
