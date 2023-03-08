package dev.caraml.store.feature.mlp;

public class FailedRequestException extends RuntimeException {

  public FailedRequestException() {
    super();
  }

  public FailedRequestException(String message) {
    super(message);
  }

  public FailedRequestException(String message, Throwable e) {
    super(message, e);
  }
}
