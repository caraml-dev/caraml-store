package dev.caraml.store.sparkjob.crd;

import lombok.Data;

import java.util.Map;

@Data
public class SparkApplicationStatus {

  private SparkApplicationState applicationState;
  private String sparkApplicationId;
  private String lastSubmissionAttemptTime;
  private String terminationTime;
  private Map<String, String> executorState;
}
