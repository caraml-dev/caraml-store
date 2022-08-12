package dev.caraml.store.sparkjob;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.kubernetes")
@Getter
@Setter
public class ClusterConfig {
  private Boolean inCluster;
}
