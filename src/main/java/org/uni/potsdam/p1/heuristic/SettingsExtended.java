package org.uni.potsdam.p1.heuristic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.EventPattern;

/**
 * <p>
 *     Similar to the Settings abstract class in the main package,
 *     this extends the operator graph architecture for the heuristic load shedding scenario
 * </p>
 * <ol>
 *   <li>Amount of records to be produced: BATCH_SIZE</li>
 *   <li>Amount of events used for calculating running averages at the OPERATORS:
 *      CONTROL_BATCH_SIZE</li>
 *   <li>Latency bound to be considered for the average processing time</li>
 *   <li>Function used to generate new records: EVENT_GENERATOR</li>
 *   <li>List containing all operators' information: OPERATORS</li>
 *   <li>Address of the kafka server if available: KAFKA_ADDRESS</li>
 *   <li>Additional kafka connect configurations</li>
 * </ol>
 */

public abstract class SettingsExtended {
    public static final int RECORDS_PER_SECOND = 100;
    public static final int CONTROL_BATCH_SIZE = 100;
    public static final int BATCH_SIZE = 1_000_0;
    public static final double LATENCY_BOUND = 0.0001;
    public static final int TIME_WINDOW = 10;
    public static final GeneratorFunction<Long, Event> EVENT_GENERATOR = index -> new Event();

    public OperatorInfo[] OPERATORS = new OperatorInfo[]{
            new OperatorInfo().withName("o1")
                    .withInputTypes("1", "2", "3", "0")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.AND("11", "0:1:3", TIME_WINDOW, new String[]{"o4", "o5"})
            ),

            new OperatorInfo().withName("o2")
                    .withInputTypes("1", "2", "3", "0")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.SEQ("21", "0|2:1|1", TIME_WINDOW, "o4"),
                    EventPattern.AND("22", "0:3", TIME_WINDOW, "o5")
            ),

            new OperatorInfo().withName("o3")
                    .withInputTypes("1", "2", "3", "0")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.SEQ("31", "2|2:1|1", TIME_WINDOW, new String[]{"o6", "o7"})
            ),

            new OperatorInfo().withName("o4")
                    .withInputTypes("11", "22")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.SEQ("41", "11|2:22|1", TIME_WINDOW, "o8")
            ),

            new OperatorInfo().withName("o5")
                    .withInputTypes("11", "21")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.SEQ("51", "11|1:21|1:11|2", TIME_WINDOW, "o8")
            ),

            new OperatorInfo().withName("o6")
                    .withInputTypes("22", "31")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.AND("61", "11:22", TIME_WINDOW, "o8"),
                    EventPattern.SEQ("62", "22|2:31|1", TIME_WINDOW, "o9")
            ),

            new OperatorInfo().withName("o7")
                    .withInputTypes("31")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    //EventPattern.FORWARD("71", "31", 10, "o9"),
                    EventPattern.SEQ("72", "0|1:1|2", TIME_WINDOW, "o12")
            ),

            new OperatorInfo().withName("o8")
                    .withInputTypes("41", "51", "61")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    EventPattern.AND("81", "41:61", TIME_WINDOW, "o10"),
                    EventPattern.SEQ("82", "51|3:61|1", TIME_WINDOW, "o11"),
                    EventPattern.SEQ("83", "61|1:41|4", TIME_WINDOW, "o12")
            ),

            new OperatorInfo().withName("o9")
                    .withInputTypes("62", "71")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                    //EventPattern.FORWARD("114", "62", 10, "o11"),
                    EventPattern.SEQ("91", "62|2:71|1", TIME_WINDOW, "o10")
            ),

            new OperatorInfo().withName("o10")
                    .withInputTypes("81")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                            EventPattern.SEQ("101", "81|6", TIME_WINDOW, (String[]) null)
                    ).toSink(),

            new OperatorInfo().withName("o11")
                    .withInputTypes("82", "83")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                            EventPattern.SEQ("111", "82|1:91|3", TIME_WINDOW, (String[]) null)
                    ).toSink(),

            new OperatorInfo().withName("o12")
                    .withInputTypes("83", "72")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                            EventPattern.SEQ("121", "72|1:83|1:72|1:83|1", TIME_WINDOW, (String[]) null)
                    ).toSink(),

            new OperatorInfo().withName("o13")
                    .withInputTypes("91")
                    .withControlBatchSize(CONTROL_BATCH_SIZE)
                    .withLatencyBound(TIME_WINDOW)
                    .withPatterns(
                            EventPattern.AND("131", "91", TIME_WINDOW, (String[]) null)
                    ).toSink()
    };
  /**
   * Execute the complex event detection job and return the system's results once finished.
   *
   * @return The execution results of the flink cluster
   * @throws Exception
   */
  public abstract JobExecutionResult execute() throws Exception;

}