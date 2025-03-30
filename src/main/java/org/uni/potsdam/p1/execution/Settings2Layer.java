package org.uni.potsdam.p1.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.uni.potsdam.p1.actors.sources.Source;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.Scope;


/**
 * <p>
 * This class stores the basic information of a data stream job to be parsed and executed
 * by an {@link OperatorGraph}.
 * Define here the following global parameters:
 * </p>
 * <ol>
 *   <li>Amount of records to be produced per second: RECORD_PER_SECOND</li>
 *   <li>Amount of events used for calculating running averages at the operators:
 *   CONTROL_BATCH_SIZE</li>
 *   <li>Amount of records to be produced: BATCH_SIZE</li>
 *   <li>Latency bound to be considered for the average processing time: LATENCY_BOUND</li>
 *   <li>Standard time window to be considered in the patterns (amount of time between the
 *   first and last events): TIME_WINDOW</li>
 *   <li>Scope of the operator graph (with global or local load shedding): GLOBAL_SCOPE</li>
 *   <li>Array containing all sources' information: SOURCES</li>
 *   <li>Array containing all operators' information: OPERATORS</li>
 * </ol>
 * <p>
 * Activating the global load shedding configuration assumes that the job is going to be
 * executed using the docker network provided in the flink-swarm.yml file present in this
 * project.
 * </p>
 * <p>
 * Make sure to define operators in the order in which they appear in the operator graph,
 * that is, only define an operator after defining all other operators that lead to it (
 * that send it events).
 * </p>
 */
public abstract class Settings2Layer {

  // GENERAL JOB INFORMATION

  public static final int RECORDS_PER_SECOND = 100;
  public static final int CONTROL_BATCH_SIZE = 100;
  public static final int BATCH_SIZE = 10_000;
  public static final double LATENCY_BOUND = 0.00055;
  public static final int TIME_WINDOW = 10;
  public static final Scope SCOPE = Scope.HYBRID;
  public static final long FACTOR = (long)(1/700.*1E9);


  // define sources
  public static final Source[] SOURCES = new Source[]{
    new Source()
      .withName("s1")
      .withOutputTypes("0 1 2 3".split(" "))
      .withBatchSize(BATCH_SIZE)
      .withRecordsPerSecond(RECORDS_PER_SECOND)
      .withDownStreamOperators("o1")
    ,
    new Source()
      .withName("s2")
      .withOutputTypes("0 1 2 3".split(" "))
      .withBatchSize(BATCH_SIZE)
      .withRecordsPerSecond(RECORDS_PER_SECOND)
      .withDownStreamOperators("o2")
  };

  // define operators
  public OperatorInfo[] OPERATORS = new OperatorInfo[]{
     new OperatorInfo()
       .withName("o1")
       .withInputTypes("0 1 2 3".split(" "))
       .withControlBatchSize(CONTROL_BATCH_SIZE)
       .withLatencyBound(LATENCY_BOUND)
       .withPatterns(
         EventPattern.SEQ("11", "0|2:1|1", TIME_WINDOW, "o3"),
         EventPattern.AND("12", "1:2:3", TIME_WINDOW, "o4"))
      .withExecutionGroup("o1")
     ,

     new OperatorInfo()
       .withName("o2")
       .withInputTypes("0 1 2 3".split(" "))
       .withControlBatchSize(CONTROL_BATCH_SIZE)
       .withLatencyBound(LATENCY_BOUND)
       .withPatterns(
         EventPattern.SEQ("21", "0|2:1|1", TIME_WINDOW, "o3"),
         EventPattern.AND("22", "1:2:3", TIME_WINDOW, "o4"))
      .withExecutionGroup("o2")
     ,

     new OperatorInfo()
      .withName("o3")
      .withInputTypes("11 21".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
        EventPattern.AND("1000", "11:21", TIME_WINDOW))
      .toSink()
     ,

     new OperatorInfo()
      .withName("o4")
      .withInputTypes("12 22".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withPatterns(
        EventPattern.AND("2000", "12:22", TIME_WINDOW))
      .toSink()

  };

  /**
   * Execute the complex event detection job and return the system's results once finished.
   *
   * @return The execution results of the flink cluster
   * @throws Exception
   */
  public abstract JobExecutionResult execute() throws Exception;

}
