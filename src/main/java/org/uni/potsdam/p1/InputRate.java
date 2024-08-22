package org.uni.potsdam.p1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;
import java.util.Scanner;

public class InputRate extends Rate<Integer> implements Serializable {

  public InputRate() {
    events = new HashMap<>();
    total = 0;
  }

  public InputRate(String... events) {
    this();
    for (String event : events) {
      countEvent(event, 0);
    }
  }

  public InputRate(Integer... events) {
    this();
    for (int i = 0; i < events.length; i++) {
      Integer current = events[i];
      countEvent(String.valueOf(i), current);
    }
  }

  @Override
  public InputRate parse(String s) throws RuntimeException {
    Scanner scanner = new Scanner(s);
//    File toDelete = new File(scanner.next());
//    if (!toDelete.delete()) {
//      throw new RuntimeException("File " + toDelete.getAbsolutePath() + "was not deleted " +
//        "successfully.");
//    }
    return new InputRate(scanner.tokens().map(Integer::valueOf).toArray(Integer[]::new));
  }

  @Override
  public boolean isValid(String key, double prob) {
    return prob < (double) events.get(key) / total;
  }

  @Override
  public void countEvent(String key, int count) {
    if (count < 0) {
      throw new IllegalArgumentException("Variable count must be a positive integer.");
    }
    events.put(key, events.getOrDefault(key, 0) + count);
    total += count;
  }

  @Override
  public int getTotal() {
    return total;
  }

  public void clear() {
    events.clear();
    total = 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputRate inputRate = (InputRate) o;
    return total == inputRate.total;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(total);
  }

  @Override
  public String toString() {
    return "InputRate{" +
      "events=" + events +
      ", total=" + total +
      '}';
  }
}
