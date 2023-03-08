package dev.caraml.store.feature.mlp;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.mlp")
@Getter
@Setter
public class ClientConfig {
  private Boolean enabled;
  private String endpoint;
  private Integer connectionTimeOutMs;
  private Integer requestTimeOutMs;
  private Boolean authEnabled;
  private String authTargetAudience;
}
