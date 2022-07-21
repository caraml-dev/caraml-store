package dev.caraml.store.config;

import io.grpc.BindableService;
import io.grpc.Status;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.function.UnaryOperator;
import lombok.Getter;
import lombok.Setter;
import net.devh.boot.grpc.server.metric.MetricCollectingServerInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "caraml")
public class StoreConfig {

  @Autowired
  public StoreConfig() {}

  /* Spark Jobs properties */
  private JobServiceConfig jobservice;

  @Bean
  JobServiceConfig getJobServiceConfig() {
    return jobservice;
  }

  private ClusterConfig kubernetes;

  @Bean
  ClusterConfig getClusterConfig() {
    return kubernetes;
  }

  private RegistryServiceConfig registryService;

  @Bean
  RegistryServiceConfig getCoreServiceConfig() {
    return registryService;
  }

  private MonitoringConfig monitoring;

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
                        monitoring.timer().percentiles().stream()
                            .mapToDouble(Double::doubleValue)
                            .toArray())
                    .minimumExpectedValue(Duration.ofMillis(monitoring.timer().minBucketMs()))
                    .maximumExpectedValue(Duration.ofMillis(monitoring.timer().maxBucketMs())),
            Status.Code.OK);
    for (final BindableService service : services) {
      metricCollector.preregisterService(service);
    }
    return metricCollector;
  }
}
