package dev.caraml.store.mlp;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.mlp.context")
@Getter
@Setter
public class ContextProviderConfig {
  private String fallbackTeam;
  private String fallbackStream;
}
