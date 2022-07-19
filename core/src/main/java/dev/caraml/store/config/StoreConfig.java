package dev.caraml.store.config;

import lombok.Getter;
import lombok.Setter;
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

  private CoreServiceConfig coreService;

  @Bean
  CoreServiceConfig getCoreServiceConfig() {
    return coreService;
  }
}
