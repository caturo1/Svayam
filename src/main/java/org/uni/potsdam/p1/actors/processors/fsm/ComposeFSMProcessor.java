package org.uni.potsdam.p1.actors.processors.fsm;

import org.uni.potsdam.p1.types.Measurement;

public abstract class ComposeFSMProcessor extends FSMProcessor {
  FSMProcessor processor1;
  FSMProcessor processor2;

  public ComposeFSMProcessor(int outputType, FSMProcessor processor1, FSMProcessor processor2) {
    super(outputType);
    this.processor1 = processor1;
    this.processor2 = processor2;
  }

  @Override
  public Measurement processElement(Measurement value) {
    Measurement result1 = getResult(processor1, value);
    Measurement result2 = getResult(processor2, value);
    if (applyAcceptanceCondition(result1, result2)) {
      String result = String.valueOf(result1) +
        result2;
      return new Measurement(patternType, result, 0);
    }
    return null;
  }

  public abstract boolean applyAcceptanceCondition(Measurement result1, Measurement result2);

  public Measurement getResult(FSMProcessor processor, Measurement value) {
    if (processor == null) {
      return null;
    }
    return processor.processElement(value);
  }
}
