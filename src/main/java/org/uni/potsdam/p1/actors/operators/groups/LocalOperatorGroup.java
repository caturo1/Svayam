package org.uni.potsdam.p1.actors.operators.groups;

import org.uni.potsdam.p1.actors.operators.LocalOperator;
import org.uni.potsdam.p1.execution.Settings;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;

import java.util.Map;

/**
 * <p>
 * Operator group to be used in local {@link Settings}. Contains only a {@link LocalOperator}
 * , which processes events and calculates and updates its own load shedding rates independently.
 * </p>
 */
public class LocalOperatorGroup extends AbstractOperatorGroup {

  public final LocalOperator operator;

  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public LocalOperatorGroup(OperatorInfo operatorInfo) {
    super(operatorInfo);
    operator = new LocalOperator(operatorInfo);
  }

  /**
   * Creates this operator's output stream and exposes it to the system.
   */
  public void createDataStream() {
    String opName = operatorInfo.name;
    String executionGroup = operatorInfo.executionGroup;

    outputDataStream = inputDataStreams
      .process(operator)
      .name("Operator_" + opName);

    if(executionGroup!=null) {
      outputDataStream.slotSharingGroup(executionGroup);
    }

  }

  public void setOutputs(Map<String, AbstractOperatorGroup> operatorGroupMap) {
    for (EventPattern pattern : operatorInfo.patterns) {
      for (String operatorName : pattern.downstreamOperators) {
        EventOutput current = operatorGroupMap.get(operatorName).toThisOperator;
        operator.setSideOutput(operatorName, current);
      }
    }
  }
}
