package dev.caraml.store.sparkjob.crd;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SparkApplicationState {
  private String state;
}
