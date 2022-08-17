package dev.caraml.store.feature;

/** Exception thrown when retrieval of an entity from the registry fails. */
public class EntityNotFoundException extends ResourceNotFoundException {
  public EntityNotFoundException(String project, String entity) {
    super(String.format("No such Entity: (project: %s, name: %s)", project, entity));
  }
}
