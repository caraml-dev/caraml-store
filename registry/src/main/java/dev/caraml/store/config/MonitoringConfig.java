package dev.caraml.store.config;

import java.util.List;

public record MonitoringConfig(Timer timer) {
  record Timer(List<Double> percentiles, Integer minBucketMs, Integer maxBucketMs) {}
}
