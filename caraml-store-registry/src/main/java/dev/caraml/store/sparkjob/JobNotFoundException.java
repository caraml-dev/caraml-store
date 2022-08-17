package dev.caraml.store.sparkjob;

/** Exception thrown when a requested spark application does not exist. */
public class JobNotFoundException extends RuntimeException {
  public JobNotFoundException(String jobId) {
    super(String.format("Job id %s does not exist", jobId));
  }
}
