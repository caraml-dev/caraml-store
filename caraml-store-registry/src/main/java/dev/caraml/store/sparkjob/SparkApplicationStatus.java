package dev.caraml.store.sparkjob;

import lombok.Data;

@Data
public class SparkApplicationStatus {

  private SparkApplicationState applicationState;
  private String sparkApplicationId;
  private String lastSubmissionAttemptTime;
  private String terminationTime;
}
