package org.uni.potsdam.p1.actors.operators.tools;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.actors.operators.groups.AbstractOperatorGroup;
import org.uni.potsdam.p1.actors.operators.groups.GlobalOperatorGroup;
import org.uni.potsdam.p1.types.outputTags.StringOutput;

import java.util.HashMap;
import java.util.Map;

/**
 * Forwards messages from Kafka to one or more operators. Messages are originated either
 * from the Coordinator (new shedding configuration) or from the respective operator's
 * analyser (stop shedding).
 */
public class Messenger extends ProcessFunction<String,String> {

  public Map<String, StringOutput> extraOutputs;

  public Messenger(Map<String, AbstractOperatorGroup> opGroups) {
    extraOutputs = new HashMap<>(opGroups.size());
    for(Map.Entry<String,AbstractOperatorGroup> entry : opGroups.entrySet()) {
      extraOutputs.put(entry.getKey(),
        ((GlobalOperatorGroup) entry.getValue()).fromMessenger);
    }
  }

  @Override
  public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
    if(s.equals("snap")) {
      collector.collect(s);
    } else {
      int index = s.indexOf(":");
      context.output(extraOutputs.get(index>0?s.substring(0,index):s),s);
    }
  }
}
