package dev.caraml.serving.monitoring;

import io.grpc.Status;
import io.micrometer.core.instrument.Timer;

public interface Metrics<ReqT, RespT> {

  void onRequestReceived(ReqT requestMessage);

  void onResponseSent(
      ReqT requestMessage, RespT responseMessage, Status.Code statusCode, Timer.Sample timerSample);
}
