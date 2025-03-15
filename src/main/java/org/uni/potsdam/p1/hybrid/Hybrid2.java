package org.uni.potsdam.p1.hybrid;

import org.uni.potsdam.p1.types.OperatorInfo;
import org.uni.potsdam.p1.variant.Variant1;

public class Hybrid2 extends Variant1 {
  /**
   * Constructs a new operator group based on an operator's information.
   *
   * @param operatorInfo This operator's attributes.
   */
  public Hybrid2(OperatorInfo operatorInfo) {
    super(operatorInfo);
    operator.setMetricsOutput("lambdaOut", toAnalyser);
    analyser.withLambdaOut();
  }
}
