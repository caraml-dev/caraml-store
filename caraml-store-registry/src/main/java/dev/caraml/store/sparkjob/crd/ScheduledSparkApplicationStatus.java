package dev.caraml.store.sparkjob.crd;

import java.util.List;
import lombok.Data;

@Data
public class ScheduledSparkApplicationStatus {
  private String lastRun;
  private String lastRunName;
  private String nextRun;
  private List<String> pastSuccessfulRunNames;
  private String scheduleState;
}
