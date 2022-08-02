package dev.caraml.store.sparkjob.crd;

import lombok.Data;

@Data
public class SecretInfo {
  private String name;
  private String path;
  private SecretType secretType;
}
