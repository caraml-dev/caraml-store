package dev.caraml.store.sparkjob;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.job-service")
@Getter
@Setter
public class JobServiceConfig {
  public record SparkJobProperties(
      String store, String namespace, SparkApplicationSpec sparkApplicationSpec) {}

  List<SparkJobProperties> streamingJobs;
}
