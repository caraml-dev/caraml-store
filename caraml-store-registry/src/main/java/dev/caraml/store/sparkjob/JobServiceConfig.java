package dev.caraml.store.sparkjob;

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

  private String namespace;
  private CommonJobProperties common;
  private List<IngestionJobTemplate> streamIngestion = new ArrayList<>();
  private List<IngestionJobTemplate> batchIngestion = new ArrayList<>();
  private HistoricalRetrievalJobTemplate historicalRetrieval;
  private DefaultStore defaultStore;
  private DeltaIngestionDataset deltaIngestionDataset;
  private boolean enableBatchJobHistory = false;
}
