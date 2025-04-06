package org.uni.potsdam.p1.actors.sources;

import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uni.potsdam.p1.types.Event;

public class FromFile implements GeneratorFunction<Long, Event> {
  public final String name;
  private String[] arr;
  public int index = 0;
  public Logger sourceLog = LoggerFactory.getLogger("sourceLog");

  public String[] getArr() {
    return arr;
  }

  public FromFile(String name) {
    this.name = name;
  }

  public void setArr(String[] arr) {
    this.arr = arr;
  }

  GeneratorFunction<Long, Event> use(String[] arg) {
    arr = arg;
    return this;
  }

  public String getNext(){
   return arr[index++];
  }

  @Override
  public Event map(Long aLong) throws Exception {
    Event result = new Event(arr[index++],aLong.toString());
    sourceLog.info(result.toString(name));
    return result;
  }
}
