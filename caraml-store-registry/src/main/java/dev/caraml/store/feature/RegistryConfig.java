package dev.caraml.store.feature;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.registry")
@Getter
@Setter
public class RegistryConfig {
  private Boolean syncIngestionJobOnSpecUpdate;
  private Long featureTableDefaultMaxAgeSeconds;
}
