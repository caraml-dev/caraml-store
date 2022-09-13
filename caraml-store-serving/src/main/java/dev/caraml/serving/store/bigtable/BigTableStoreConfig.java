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

@Configuration
@ConfigurationProperties(prefix = "caraml.store.bigtable")
@ConditionalOnProperty("caraml.store.bigtable.enabled")
@Getter
@Setter
public class BigTableStoreConfig {
  private String projectId;
  private String instanceId;
  private String appProfileId;

  @Bean
  public OnlineRetriever getRetriever() {
    try {
      BigtableDataClient client =
          BigtableDataClient.create(
              BigtableDataSettings.newBuilder()
                  .setProjectId(projectId)
                  .setInstanceId(instanceId)
                  .setAppProfileId(appProfileId)
                  .build());
      return new BigTableOnlineRetriever(client);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
