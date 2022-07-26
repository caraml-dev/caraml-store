package dev.caraml.store.monitoring;

import io.grpc.BindableService;
import io.grpc.Status;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import lombok.Getter;
import lombok.Setter;
import net.devh.boot.grpc.server.metric.MetricCollectingServerInterceptor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.monitoring")
@Getter
@Setter
public class MonitoringConfig {
  record TimerConfig(List<Double> percentiles, Integer minBucketMs, Integer maxBucketMs) {}

  private TimerConfig timer;

  @Bean
  public MetricCollectingServerInterceptor metricCollectingServerInterceptor(
      final MeterRegistry registry, final Collection<BindableService> services) {
    final MetricCollectingServerInterceptor metricCollector =
        new MetricCollectingServerInterceptor(
            registry,
            UnaryOperator.identity(),
            timer ->
                timer
                    .publishPercentiles(
                        getTimer().percentiles().stream()
                            .mapToDouble(Double::doubleValue)
                            .toArray())
                    .minimumExpectedValue(Duration.ofMillis(getTimer().minBucketMs()))
                    .maximumExpectedValue(Duration.ofMillis(getTimer().maxBucketMs())),
            Status.Code.OK);
    for (final BindableService service : services) {
      metricCollector.preregisterService(service);
    }
    return metricCollector;
  }
}
