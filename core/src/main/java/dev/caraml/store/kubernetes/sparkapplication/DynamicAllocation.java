package dev.caraml.store.kubernetes.sparkapplication;

import lombok.Data;

@Data
public class DynamicAllocation {

  private Boolean enabled;
  private Integer initialExecutors;
  private Integer minExecutors;
  private Integer maxExecutors;
}
