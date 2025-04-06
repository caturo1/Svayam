package org.uni.potsdam.p1.execution;

import org.uni.potsdam.p1.actors.sources.FromFile;
import org.uni.potsdam.p1.types.Event;

import java.time.Instant;
import java.util.regex.Pattern;

public class FileExtractor extends FromFile {
  Pattern pattern = Pattern.compile(",");

  public FileExtractor(String name) {
    super(name);
  }

  @Override
  public Event map(Long value) throws Exception {
    String[] row = pattern.split(getNext());
    try{
      Event result = new Event(row[0].toString(), row[2].toString(), Instant.ofEpochMilli(Long.parseLong(row[1])));
        //      Event result = new Event(row[0], row[3],Instant.ofEpochMilli(Long.parseLong(row[1])));
      sourceLog.info(result.toString(name));
      return result;
    } catch(NumberFormatException e) {
      System.err.println("Problem with "+index);
      e.printStackTrace();
      throw e;
    }
  }
}
