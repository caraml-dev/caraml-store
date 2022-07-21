package dev.caraml.store.kubernetes.sparkapplication;

import lombok.Data;

@Data
public class SparkApplicationStatus {

  private SparkApplicationState applicationState;
  private String sparkApplicationId;
  private String lastSubmissionAttemptTime;
  private String terminationTime;
}
