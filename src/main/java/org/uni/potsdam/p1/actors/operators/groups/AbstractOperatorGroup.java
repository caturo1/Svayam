package org.uni.potsdam.p1.actors.operators.groups;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.uni.potsdam.p1.types.Event;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;

import java.util.Map;

/**
 * <p>
 * This class represents the main abstraction of an operator to be used in the
 * {@link org.uni.potsdam.p1.execution.OperatorGraph}.
 * </p>
 * <p>
 * An OperatorGroup stores the operator's input and
 * output data streams, its connecting outputTag (the {@link EventOutput} instance
 * used to reach this operator from other operators), the core operator's information as
 * well as the operator's processor and possibly additional control structures.
 * </p>
 * <p>
 * Variants of this class ({@link LocalOperatorGroup} or {@link GlobalOperatorGroup}) are
 * parsed from the operator information present in the {@link org.uni.potsdam.p1.execution.Settings}
 * class and used in the operator's data graph to connect the different data streams
 * generated in the flink cluster.
 * </p>
 */
public abstract class AbstractOperatorGroup {

  public final EventOutput toThisOperator;
  public final OperatorInfo operatorInfo;
  public DataStream<Event> inputDataStreams;
  public SingleOutputStreamOperator<Event> outputDataStream;

  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public AbstractOperatorGroup(OperatorInfo operatorInfo) {
    this.operatorInfo = operatorInfo;
    toThisOperator = new EventOutput("out_to_operator_" + operatorInfo.name);
  }


  /**
   * Adds or expands this operator's input stream with a new data stream originated from a {@link org.uni.potsdam.p1.actors.sources.Source}.
   *
   * @param inputStream New inputStream of measurements to be added to this operator's inputs (from Source).
   */
  public void addInputStream(DataStream<Event> inputStream) {
    if (inputDataStreams == null) {
      inputDataStreams = inputStream;
    } else {
      inputDataStreams = inputDataStreams.union(inputStream);
    }
  }

  /**
   * Adds or expands this operator's input stream with a new data stream originated from a {@link AbstractOperatorGroup}.
   *
   * @param inputStream New inputStream of measurements to be added to this operator's inputs (from another operator's side output).
   */
  public void addInputStream(SingleOutputStreamOperator<Event> inputStream) {
    DataStream<Event> stream = inputStream.getSideOutput(toThisOperator);
    if (inputDataStreams == null) {
      inputDataStreams = stream;
    } else {
      inputDataStreams = inputDataStreams.union(stream);
    }
  }

  /**
   * Add side outputs connections to other operators using a connection operator group map
   * provided by the {@link org.uni.potsdam.p1.execution.OperatorGraph}.
   *
   * @param operatorGroupMap Map linking each operator group with its own {@link EventOutput}
   *                         instance.
   */
  public abstract void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap);
}
