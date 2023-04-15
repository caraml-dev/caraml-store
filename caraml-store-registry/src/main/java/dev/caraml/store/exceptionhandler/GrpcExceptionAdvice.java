package dev.caraml.store.exceptionhandler;

import dev.caraml.store.feature.ResourceNotFoundException;
import dev.caraml.store.mlp.FailedRequestException;
import dev.caraml.store.sparkjob.JobNotFoundException;
import dev.caraml.store.sparkjob.SparkOperatorApiException;
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
  public Status handleResourceNotFound(ResourceNotFoundException e) {
    return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleJobNotFound(JobNotFoundException e) {
    return Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleSparkOperatorApiException(SparkOperatorApiException e) {
    return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleFailedRequestException(FailedRequestException e) {
    return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
  }

  @GrpcExceptionHandler
  public Status handleInternalError(Exception e) {
    log.debug(e.getMessage());
    return Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
  }
}
