package org.uni.potsdam.p1.types.outputTags;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import org.uni.potsdam.p1.types.Event;

/**
 * Concrete type for an OutputTag of Measurements. Used to deal with Flink's serialization
 * issues
 */
public class EventOutput extends OutputTag<Event> {
  public EventOutput(String id) {
    super(id);
  }

  public EventOutput(String id, TypeInformation<Event> typeInfo) {
    super(id, typeInfo);
  }
}
