package dev.caraml.serving.monitoring;

import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest;
import io.grpc.Status;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class ServingMetrics<ReqT, RespT> implements Metrics<ReqT, RespT> {

  private final MeterRegistry registry;
  private final String methodName;

  public ServingMetrics(MeterRegistry registry, String methodName) {
    this.registry = registry;
    this.methodName = methodName;
  }

  private Timer newServingLatencyTimer(String project, Status.Code statusCode) {
    return Timer.builder("feast_serving_request_latency_seconds")
        .tag("method", methodName)
        .tag("project", project)
        .tag("status_code", statusCode.name())
        .register(registry);
  }

  @Override
  public void onRequestReceived(ReqT requestMessage) {}

  @Override
  public void onResponseSent(
      ReqT requestMessage,
      RespT responseMessage,
      Status.Code statusCode,
      Timer.Sample timerSample) {
    if (requestMessage instanceof GetOnlineFeaturesRequest featureRequest) {
      timerSample.stop(newServingLatencyTimer(featureRequest.getProject(), statusCode));
    }
  }
}
