package dev.caraml.store.sparkjob.crd;

import java.util.Map;
import lombok.Data;

@Data
public class SparkApplicationStatus {

  private SparkApplicationState applicationState;
  private String sparkApplicationId;
  private String lastSubmissionAttemptTime;
  private String terminationTime;
  private Map<String, String> executorState;
}
