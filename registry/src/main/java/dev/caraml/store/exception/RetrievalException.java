package dev.caraml.store.exception;

/** Exception thrown when retrieval of a spec from the registry fails. */
public class RetrievalException extends RuntimeException {
  public RetrievalException() {
    super();
  }

  public RetrievalException(String message) {
    super(message);
  }

  public RetrievalException(String message, Throwable cause) {
    super(message, cause);
  }
}
