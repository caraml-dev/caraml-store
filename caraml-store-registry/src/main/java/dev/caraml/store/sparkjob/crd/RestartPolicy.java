package dev.caraml.store.sparkjob.crd;

import lombok.Data;

@Data
public class RestartPolicy {

  private String type;
  private Integer onFailureRetries;
  private Integer onFailureRetryInterval;
  private Integer onSubmissionFailureRetries;
  private Integer onSubmissionFailureRetryInterval;
}
