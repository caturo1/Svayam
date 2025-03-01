package org.uni.potsdam.p1.actors.operators.groups;

import org.uni.potsdam.p1.actors.operators.BasicOperator;
import org.uni.potsdam.p1.types.EventPattern;
import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.types.outputTags.EventOutput;

import java.util.Map;

public class BasicOperatorGroup extends AbstractOperatorGroup {

  public final BasicOperator operator;

  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public BasicOperatorGroup(OperatorInfo operatorInfo) {
    super(operatorInfo);
    operator = new BasicOperator(operatorInfo);
  }

  /**
   * Creates this operator's output stream and exposes it to the system.
   */
  public void createDataStream() {
    String opName = operatorInfo.name;

    outputDataStream = inputDataStreams
      .process(operator)
      .slotSharingGroup(opName)
      .name("Operator_" + opName);

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
