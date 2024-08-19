package org.uni.potsdam.p1;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Scanner;

public class Rates implements Serializable {

  public double E1;
  public double E2;
  public double E3;
  public double E4;

  public Rates() {
  }

  public Rates(double E1, double E2, double E3, double E4) {
    this.E1 = E1;
    this.E2 = E2;
    this.E3 = E3;
    this.E4 = E4;
  }

  public static Rates parse(String s) throws RuntimeException {
    Scanner scanner = new Scanner(s);
    double value1 = scanner.nextDouble();
    double value2 = scanner.nextDouble();
    double value3 = scanner.nextDouble();
    double value4 = scanner.nextDouble();
    File toDelete = new File(scanner.next());
    if (!toDelete.delete()) {
      throw new RuntimeException("File " + toDelete.getAbsolutePath() + "was not deleted " +
        "successfully.");
    }
    return new Rates(value1, value2, value3, value4);
  }

  @Override
  public String toString() {
    return "Rates{" +
      "E1=" + E1 +
      ", E2=" + E2 +
      ", E3=" + E3 +
      ", E4=" + E4 +
      '}';
  }

  public double getEvent(int index) {
    return Arrays.asList(E1, E2, E3, E4).get(index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Rates rates = (Rates) o;
    return Double.compare(E1, rates.E1) == 0 && Double.compare(E2, rates.E2) == 0 && Double.compare(E3, rates.E3) == 0 && Double.compare(E4, rates.E4) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(E1, E2, E3, E4);
  }

}
