package dev.caraml.serving.monitoring;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.monitoring")
@Getter
@Setter
public class MonitoringConfig {
  record TimerConfig(List<Double> percentiles, Integer minBucketMs, Integer maxBucketMs) {}

  record EntityCountDistributionConfig(List<Double> percentiles, Integer maxExpectedCount) {}

  private TimerConfig timer;
  private EntityCountDistributionConfig entityCountDistribution;
}
