package org.uni.potsdam.p1.types;

import org.assertj.core.data.Percentage;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;

public class MeasurementTest {

  @DisplayName("measurements is created with an id between 0 and 3 and near current time")
  @Test
  public void testMeasurementCreation() {
    // given
    long currentTime = System.nanoTime();
    // when
    Measurement newMeasurement = new Measurement();
    // then
    assertThat(newMeasurement.machineId).isBetween(0, 4);
    assertThat(newMeasurement.eventTime).isCloseTo(currentTime,
      Percentage.withPercentage(10e-5));
  }
}