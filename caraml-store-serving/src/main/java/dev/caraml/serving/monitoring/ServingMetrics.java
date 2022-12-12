package dev.caraml.serving.monitoring;

import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;

public class ServingMetrics<ReqT, RespT> implements Metrics<ReqT, RespT> {

  private final MeterRegistry registry;
  private final String methodName;
  private final MonitoringConfig config;

  public ServingMetrics(MeterRegistry registry, String methodName, MonitoringConfig config) {
    this.registry = registry;
    this.methodName = methodName;
    this.config = config;
  }

  private Timer newServingLatencyTimer(String project, Status.Code statusCode) {
    return Timer.builder("caraml_serving_request_latency_seconds")
        .tag("method", methodName)
        .tag("project", project)
        .tag("status_code", statusCode.name())
        .publishPercentiles(
            config.getTimer().percentiles().stream().mapToDouble(Double::doubleValue).toArray())
        .minimumExpectedValue(Duration.ofMillis(config.getTimer().minBucketMs()))
        .maximumExpectedValue(Duration.ofMillis(config.getTimer().maxBucketMs()))
        .register(registry);
  }

  private Counter newServingRequestCounter(String project) {
    return Counter.builder("caraml_serving_grpc_request_count")
        .tag("method", methodName)
        .tag("project", project)
        .register(registry);
  }

  private Counter newServingResponseCounter(String project, Status.Code statusCode) {
    return Counter.builder("caraml_serving_grpc_response_count")
        .tag("method", methodName)
        .tag("project", project)
        .tag("status_code", statusCode.name())
        .register(registry);
  }

  private List<Counter> newEntityCounters(GetOnlineFeaturesRequest featureRequest) {
    return featureRequest.getFeaturesList().stream()
        .map(FeatureReference::getFeatureTable)
        .distinct()
        .map(
            table ->
                Counter.builder("caraml_serving_entity_count")
                    .tag("project", featureRequest.getProject())
                    .tag("feature_table", table)
                    .register(registry))
        .toList();
  }

  @Override
  public void onRequestReceived(ReqT requestMessage) {
    if (requestMessage instanceof GetOnlineFeaturesRequest featureRequest) {
      String project = featureRequest.getProject();
      newServingRequestCounter(project).increment();
      newEntityCounters(featureRequest)
          .forEach(counter -> counter.increment(featureRequest.getEntityRowsCount()));
    }
  }

  @Override
  public void onResponseSent(
      ReqT requestMessage,
      RespT responseMessage,
      Status.Code statusCode,
      Timer.Sample timerSample) {
    if (requestMessage instanceof GetOnlineFeaturesRequest featureRequest) {
      String project = featureRequest.getProject();
      timerSample.stop(newServingLatencyTimer(project, statusCode));
      newServingResponseCounter(project, statusCode).increment();
    }
  }
}
