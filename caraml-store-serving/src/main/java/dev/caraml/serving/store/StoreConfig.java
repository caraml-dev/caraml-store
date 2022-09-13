package dev.caraml.serving.store;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.store")
@Getter
@Setter
public class StoreConfig {

  enum StoreType {
    BIGTABLE,
    REDIS,
    REDIS_CLUSTER;
  }

  private StoreType type;
  private Map<String, String> config;
}
