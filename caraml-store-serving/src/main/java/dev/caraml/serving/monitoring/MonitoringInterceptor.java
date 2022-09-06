package dev.caraml.serving.monitoring;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MonitoringInterceptor implements ServerInterceptor {

  private final MeterRegistry registry;

  public MonitoringInterceptor(MeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    Timer.Sample timerSample = Timer.start(registry);
    final ServerCallWithMetricCollection<ReqT, RespT> serverCall =
        new ServerCallWithMetricCollection<>(call);
    String fullMethodName = call.getMethodDescriptor().getFullMethodName();
    String methodName = fullMethodName.substring(fullMethodName.indexOf("/") + 1);
    Metrics<ReqT, RespT> metrics = new ServingMetrics<>(registry, methodName);

    return new ServerCallWithMetricCollectionListener<>(
        next.startCall(serverCall, headers),
        metrics,
        serverCall::getResponseCode,
        serverCall::getResponseMessage,
        timerSample);
  }
}
