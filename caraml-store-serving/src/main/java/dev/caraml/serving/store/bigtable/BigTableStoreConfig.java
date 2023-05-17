package dev.caraml.serving.store.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import dev.caraml.serving.store.OnlineRetriever;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.threeten.bp.Duration;

@Configuration
@ConfigurationProperties(prefix = "caraml.store.bigtable")
@ConditionalOnProperty(prefix = "caraml.store", name = "active", havingValue = "bigtable")
@Getter
@Setter
public class BigTableStoreConfig {
  private String projectId;
  private String instanceId;
  private String appProfileId;
  private Boolean enableClientSideMetrics;
  private Long timeoutMs;

  @Bean
  public OnlineRetriever getRetriever() {
    try {
      BigtableDataSettings.Builder builder =
          BigtableDataSettings.newBuilder()
              .setProjectId(projectId)
              .setInstanceId(instanceId)
              .setAppProfileId(appProfileId);
      if (timeoutMs > 0) {
        builder
            .stubSettings()
            .readRowsSettings()
            .retrySettings()
            .setTotalTimeout(Duration.ofMillis(timeoutMs));
      }
      BigtableDataSettings settings = builder.build();
      if (enableClientSideMetrics) {
        BigtableDataSettings.enableBuiltinMetrics();
      }
      BigtableDataClient client = BigtableDataClient.create(settings);
      return new BigTableOnlineRetriever(client);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
