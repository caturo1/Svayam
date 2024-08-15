package org.uni.potsdam.p1.types;

import java.io.Serializable;

public class Measurement implements Serializable {
  public final int machineId;
  public final long eventTime;

  public Measurement() {
    machineId = (int) (Math.random() * 4);
    eventTime = System.nanoTime();
  }
}
