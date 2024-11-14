package org.uni.potsdam.p1.types.outputTags;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Measurement;

/**
 * Concrete type for an OutputTag of Measurements. Used to deal with Flink's serialization
 * issues
 */
public class MeasurementOutput extends OutputTag<Measurement> {
  public MeasurementOutput(String id) {
    super(id);
  }

  public MeasurementOutput(String id, TypeInformation<Measurement> typeInfo) {
    super(id, typeInfo);
  }
}
