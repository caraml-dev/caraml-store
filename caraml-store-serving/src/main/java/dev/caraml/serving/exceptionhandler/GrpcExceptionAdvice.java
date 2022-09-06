package dev.caraml.serving.exceptionhandler;

import dev.caraml.serving.featurespec.FeatureRefNotFoundException;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@GrpcAdvice
@Slf4j
public class GrpcExceptionAdvice {

  @GrpcExceptionHandler
  public Status handleInvalidArgument(IllegalArgumentException e) {
    return Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleUnsupportedOperation(UnsupportedOperationException e) {
    return Status.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleFeatureRefNotFound(FeatureRefNotFoundException e) {
    return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleInternalError(Exception e) {
    log.debug(e.getMessage());
    return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
  }
}
