package dev.caraml.store.sparkjob.crd;

import lombok.Data;

@Data
public class SparkApplicationStatus {

  private SparkApplicationState applicationState;
  private String sparkApplicationId;
  private String lastSubmissionAttemptTime;
  private String terminationTime;
}
