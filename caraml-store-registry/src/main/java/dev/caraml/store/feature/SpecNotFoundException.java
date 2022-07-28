package dev.caraml.store.feature;

/** Exception thrown when retrieval of a spec from the registry fails. */
public class SpecNotFoundException extends RuntimeException {
  public SpecNotFoundException() {
    super();
  }

  public SpecNotFoundException(String message) {
    super(message);
  }

  public SpecNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
