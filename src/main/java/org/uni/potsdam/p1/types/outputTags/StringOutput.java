package org.uni.potsdam.p1.types.outputTags;

import org.apache.flink.util.OutputTag;


/**
 * Concrete type for an OutputTag of Strings. Used to deal with Flink's serialization
 * issues
 */
public class StringOutput extends OutputTag<String> {
  public StringOutput(String id) {
    super(id);
  }
}
