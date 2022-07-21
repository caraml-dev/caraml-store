package dev.caraml.store.metrics;

import io.prometheus.client.Histogram;

public class GrpcMetrics {

  public static final Histogram requestLatency =
      Histogram.build()
          .name("feast_core_request_latency_seconds")
          .labelNames("service", "method", "status_code")
          .help("Request latency in seconds")
          .register();
}
