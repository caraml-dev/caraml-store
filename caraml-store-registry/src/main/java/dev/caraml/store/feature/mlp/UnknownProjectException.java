package dev.caraml.store.feature.mlp;

public class UnknownProjectException extends RuntimeException {

  public UnknownProjectException() {
    super();
  }

  public UnknownProjectException(String message) {
    super(message);
  }

  public UnknownProjectException(String message, Throwable e) {
    super(message, e);
  }
}
