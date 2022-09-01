package dev.caraml.store.sparkjob.crd;

import lombok.Data;

@Data
public class ScheduledSparkApplicationSpec {

  private final String schedule;
  private final SparkApplicationSpec template;
}
