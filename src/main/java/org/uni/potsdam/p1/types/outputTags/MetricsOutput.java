package org.uni.potsdam.p1.types.outputTags;

import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Metrics;

/**
 * Concrete type for an OutputTag of Metrics. Used to deal with Flink's serialization
 * issues
 */
public class MetricsOutput extends OutputTag<Metrics> {

  public MetricsOutput(String id) {
    super(id);
  }
}
