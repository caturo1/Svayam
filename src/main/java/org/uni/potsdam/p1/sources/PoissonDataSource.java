package org.uni.potsdam.p1.sources;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.uni.potsdam.p1.types.Measurement;

import java.util.concurrent.TimeUnit;

public class PoissonDataSource implements FlatMapFunction<Measurement, Measurement> {
  @Override
  public void flatMap(Measurement measurement, Collector<Measurement> collector) throws Exception {
    PoissonDistribution prob = new PoissonDistribution(2297);

    int amount = Math.min(prob.sample(),5000);

    for(int i =0 ; i < amount; i++) {
      collector.collect(new Measurement());
    }

    TimeUnit.SECONDS.sleep(1);
  }
}
