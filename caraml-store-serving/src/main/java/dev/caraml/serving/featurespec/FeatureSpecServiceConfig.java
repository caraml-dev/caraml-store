package dev.caraml.serving.featurespec;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.registry")
@Getter
@Setter
public class FeatureSpecServiceConfig {

  private String host;
  private Integer port;
  private CacheProperties cache;
}
