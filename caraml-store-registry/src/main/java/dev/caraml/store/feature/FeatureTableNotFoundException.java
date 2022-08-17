package dev.caraml.store.feature;

/** Exception thrown when retrieval of a spec from the registry fails. */
public class FeatureTableNotFoundException extends ResourceNotFoundException {
  public FeatureTableNotFoundException(String project, String featureTable) {
    super(String.format("No such Feature Table: (project: %s, name: %s)", project, featureTable));
  }
}
