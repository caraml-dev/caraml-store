package dev.caraml.store.feature;

/** Exception thrown when retrieval of a spec from the registry fails. */
public class ResourceNotFoundException extends RuntimeException {
  public ResourceNotFoundException() {
    super();
  }

  public ResourceNotFoundException(String message) {
    super(message);
  }

  public ResourceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
