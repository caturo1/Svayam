package org.uni.potsdam.p1.execution;

import org.apache.flink.api.common.JobExecutionResult;
import org.uni.potsdam.p1.actors.sources.Source;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.Scope;

/**
 * <p>
 * This class stores the basic information of a data stream job to be parsed and executed
 * by a {@link org.uni.potsdam.p1.execution.OperatorGraph} for the heuristic load shedding scenario.
 * Define here the following global parameters:
 * </p>
 * <ol>
 *   <li>Amount of records to be produced per second: RECORDS_PER_SECOND</li>
 *   <li>Amount of events used for calculating running averages at the operators:
 *   CONTROL_BATCH_SIZE</li>
 *   <li>Amount of records to be produced: BATCH_SIZE</li>
 *   <li>Latency bound to be considered for the average processing time: LATENCY_BOUND</li>
 *   <li>Standard time window to be considered in the patterns (amount of time between the
 *   first and last events): TIME_WINDOW</li>
 *   <li>Scope of the operator graph (with global or local load shedding): SCOPE</li>
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
public abstract class Settings3Layer {

  // GENERAL JOB INFORMATION
  public static final int RECORDS_PER_SECOND = 1000;
  public static final int CONTROL_BATCH_SIZE = 1000;
  public static final int BATCH_SIZE = 50_000;
  public static final double LATENCY_BOUND = 0.000055;
  public static final int TIME_WINDOW = 5;
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
      .withDownStreamOperators(new String[]{"o2", "o3"})
  };

  // define operators
  public OperatorInfo[] OPERATORS = new OperatorInfo[]{
    new OperatorInfo()
      .withName("o1")
      .withInputTypes("0 1 3".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o1")
      .withPatterns(
        EventPattern.AND("11", "0:1:3", TIME_WINDOW, new String[]{"o4", "o5"})
      ),

    new OperatorInfo()
      .withName("o2")
      .withInputTypes("0 1 3".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o2")
      .withPatterns(
        EventPattern.SEQ("21", "0|2:1|1", TIME_WINDOW, "o4"),
        EventPattern.OR("22", "0:3", TIME_WINDOW, new String[] {"o5", "o6"})
      ),

    new OperatorInfo()
      .withName("o3")
      .withInputTypes("1 2".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o3")
      .withPatterns(
        EventPattern.SEQ("31", "2|2:1|1", TIME_WINDOW, new String[]{"o6", "o7"})
      ),

    new OperatorInfo()
      .withName("o4")
      .withInputTypes("11 21".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o4")
      .withPatterns(
        EventPattern.SEQ("41", "11|2:21|1", TIME_WINDOW, new String[]{"o10","o12"})
      ),

    new OperatorInfo()
      .withName("o5")
      .withInputTypes("11 22".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o5")
      .withPatterns(
        EventPattern.SEQ("51", "11|1:22|2", TIME_WINDOW, "o10")
      ),

    new OperatorInfo()
      .withName("o6")
      .withInputTypes("31 22".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o6")
      .withPatterns(
        EventPattern.AND("61", "31:22", TIME_WINDOW, new String[]{"o10","o11"}),
        EventPattern.SEQ("62", "22|2:31|1", TIME_WINDOW, "o12")
      ),

    new OperatorInfo()
      .withName("o7")
      .withInputTypes("11 31".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o7")
      .withPatterns(
        EventPattern.OR("71", "31|1:11|2", TIME_WINDOW, new String[]{"o11", "o12"})
      ),


    new OperatorInfo()
      .withName("o10")
      .withInputTypes("41 51 61".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o10")
      .withPatterns(
        EventPattern.AND("101", "41:51:61", TIME_WINDOW)
      )
      .toSink(),

    new OperatorInfo()
      .withName("o11")
      .withInputTypes("61 71".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o11")
      .withPatterns(
        EventPattern.SEQ("111", "61|1:71|2", TIME_WINDOW)
      )
      .toSink(),

    new OperatorInfo()
      .withName("o12")
      .withInputTypes("41 62 71".split(" "))
      .withControlBatchSize(CONTROL_BATCH_SIZE)
      .withLatencyBound(LATENCY_BOUND)
      .withExecutionGroup("o12")
      .withPatterns(
        EventPattern.AND("121", "41:71", TIME_WINDOW),
        EventPattern.SEQ("121", "71|2:62|1", TIME_WINDOW)
        
      )
      .toSink(),
  };

  /**
   * Execute the complex event detection job and return the system's results once finished.
   *
   * @return The execution results of the flink cluster
   * @throws Exception
   */
  public abstract JobExecutionResult execute() throws Exception;
}