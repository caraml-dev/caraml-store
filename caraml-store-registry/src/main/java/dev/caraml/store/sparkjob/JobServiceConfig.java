package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import java.util.ArrayList;
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
  public record IngestionJobProperties(
      String store, String namespace, SparkApplicationSpec sparkApplicationSpec) {}

  public record HistoricalRetrievalJobProperties(
      String namespace, SparkApplicationSpec sparkApplicationSpec) {}

  List<IngestionJobProperties> streamIngestion = new ArrayList<>();
  List<IngestionJobProperties> batchIngestion = new ArrayList<>();
  HistoricalRetrievalJobProperties historicalRetrieval;
}
