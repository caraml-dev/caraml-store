package dev.caraml.serving.featurespec;

import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;

public class FeatureRefNotFoundException extends RuntimeException {

  public FeatureRefNotFoundException(String projectName, FeatureReference featureReference) {
    super(
        String.format(
            "%s:%s is not found in the registry", projectName, featureReference.getName()));
  }
}
