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

  public record HistoricalRetrievalJobProperties(SparkApplicationSpec sparkApplicationSpec) {}

  private String namespace;
  private List<IngestionJobProperties> streamIngestion = new ArrayList<>();
  private List<IngestionJobProperties> batchIngestion = new ArrayList<>();
  private HistoricalRetrievalJobProperties historicalRetrieval;
}
