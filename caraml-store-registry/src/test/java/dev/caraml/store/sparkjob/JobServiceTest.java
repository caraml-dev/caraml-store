package dev.caraml.store.sparkjob;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import dev.caraml.store.feature.Entity;
import dev.caraml.store.feature.EntityRepository;
import dev.caraml.store.feature.FeatureTable;
import dev.caraml.store.feature.FeatureTableRepository;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import dev.caraml.store.protobuf.types.ValueProto;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JobServiceTest {

  @Mock private EntityRepository entityRepository;
  @Mock private FeatureTableRepository tableRepository;
  @Mock private SparkOperatorApi api;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    entityRepository = mock(EntityRepository.class);
    api = mock(SparkOperatorApi.class);
  }

  @Test
  public void shouldCreateBatchIngestionJob() throws IOException, ParseException {
    List<JobServiceConfig.IngestionJobProperties> jobs = new ArrayList<>();
    JobServiceConfig properties = new JobServiceConfig();
    properties.setBatchIngestion(jobs);
    JobServiceConfig.IngestionJobProperties batchJobProperty =
        new JobServiceConfig.IngestionJobProperties(
            "store", "spark-operator", new SparkApplicationSpec());
    jobs.add(batchJobProperty);
    JobService jobservice = new JobService(properties, entityRepository, tableRepository, api);
    FeatureTableSpec.Builder builder = FeatureTableSpec.newBuilder();
    String project = "project";
    String jsonString;
    try (InputStream featureTableSpecFile =
        getClass().getClassLoader().getResourceAsStream("feature-tables/batch.json")) {
      jsonString = new String(featureTableSpecFile.readAllBytes(), StandardCharsets.UTF_8);
      JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builder);
    }
    builder.setOnlineStore(OnlineStoreProto.OnlineStore.newBuilder().setName("store").build());
    FeatureTableSpec spec = builder.build();
    Timestamp ingestionStart = Timestamps.parse("2022-08-01T01:00:00.00Z");
    Timestamp ingestionEnd = Timestamps.parse("2022-08-02T01:00:00.00Z");
    when(entityRepository.findEntityByNameAndProject_Name("entity1", project))
        .thenReturn(
            new Entity("entity1", "", ValueProto.ValueType.Enum.STRING, Collections.emptyMap()));
    FeatureTable featureTable = FeatureTable.fromProto(project, spec, entityRepository);
    when(tableRepository.findFeatureTableByNameAndProject_NameAndIsDeletedFalse(
            "batch_feature_table", project))
        .thenReturn(Optional.of(featureTable));
    jobservice.createOrUpdateBatchIngestionJob(
        project, "batch_feature_table", ingestionStart, ingestionEnd);
    SparkApplication expectedSparkApplication = new SparkApplication();
    V1ObjectMeta expectedMetadata = new V1ObjectMeta();
    expectedMetadata.setLabels(
        Map.of(
            "caraml.dev/table", "batch_feature_table",
            "caraml.dev/project", "project",
            "caraml.dev/type", "OFFLINE_TO_ONLINE_JOB"));
    expectedMetadata.setNamespace("spark-operator");
    expectedMetadata.setName("caraml-58c06c7994b14e1e88cb03ef2a392faa");
    expectedSparkApplication.setMetadata(expectedMetadata);
    SparkApplicationSpec expectedSparkApplicationSpec = new SparkApplicationSpec();
    expectedSparkApplicationSpec.addArgument(
        "feature-table",
        "{\"project\":\"project\",\"name\":\"batch_feature_table\",\"labels\":{},\"maxAge\":0,\"entities\":[{\"name\":\"entity1\",\"type\":\"STRING\"}],\"features\":[{\"name\":\"feature1\",\"type\":\"INT64\"}]}");
    expectedSparkApplicationSpec.addArgument(
        "source",
        "{\"type\":\"BATCH_BIGQUERY\",\"eventTimestampColumn\":\"event_timestamp\",\"bigqueryOptions\":{}}");
    expectedSparkApplicationSpec.addArgument("start", "2022-08-01T01:00:00Z");
    expectedSparkApplicationSpec.addArgument("end", "2022-08-02T01:00:00Z");
    expectedSparkApplication.setSpec(expectedSparkApplicationSpec);
    verify(api, times(1)).create(expectedSparkApplication);
  }

  @Test
  public void shouldCreateStreamingJob() throws IOException, SparkOperatorApiException {
    List<JobServiceConfig.IngestionJobProperties> jobs = new ArrayList<>();
    JobServiceConfig properties = new JobServiceConfig();
    properties.setStreamIngestion(jobs);
    JobServiceConfig.IngestionJobProperties streamJobProperty =
        new JobServiceConfig.IngestionJobProperties(
            "store", "spark-operator", new SparkApplicationSpec());
    jobs.add(streamJobProperty);
    JobService jobservice = new JobService(properties, entityRepository, tableRepository, api);
    FeatureTableSpec.Builder builder = FeatureTableSpec.newBuilder();
    String project = "project";
    String jsonString;
    try (InputStream featureTableSpecFile =
        getClass().getClassLoader().getResourceAsStream("feature-tables/streaming.json")) {
      jsonString = new String(featureTableSpecFile.readAllBytes(), StandardCharsets.UTF_8);
      JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builder);
    }
    builder.setOnlineStore(OnlineStoreProto.OnlineStore.newBuilder().setName("store").build());
    FeatureTableSpec spec = builder.build();

    when(entityRepository.findEntityByNameAndProject_Name("entity1", project))
        .thenReturn(
            new Entity("entity1", "", ValueProto.ValueType.Enum.STRING, Collections.emptyMap()));
    jobservice.createOrUpdateStreamingIngestionJob(project, spec);
    SparkApplication expectedSparkApplication = new SparkApplication();
    V1ObjectMeta expectedMetadata = new V1ObjectMeta();
    expectedMetadata.setLabels(
        Map.of(
            "caraml.dev/table", "streaming_feature_table",
            "caraml.dev/project", "project",
            "caraml.dev/type", "STREAM_TO_ONLINE_JOB"));
    expectedMetadata.setNamespace("spark-operator");
    expectedMetadata.setName("caraml-580e67b7b225337efeca46c7df15938c");
    expectedSparkApplication.setMetadata(expectedMetadata);
    SparkApplicationSpec expectedSparkApplicationSpec = new SparkApplicationSpec();
    expectedSparkApplicationSpec.addArgument(
        "feature-table",
        "{\"project\":\"project\",\"name\":\"streaming_feature_table\",\"labels\":{},\"maxAge\":0,\"entities\":[{\"name\":\"entity1\",\"type\":\"STRING\"}],\"features\":[{\"name\":\"feature1\",\"type\":\"FLOAT\"}]}");
    expectedSparkApplicationSpec.addArgument(
        "source",
        "{\"type\":\"STREAM_KAFKA\",\"eventTimestampColumn\":\"event_timestamp\",\"kafkaOptions\":{\"bootstrapServers\":\"kafka:9102\",\"topic\":\"topic\",\"messageFormat\":{\"protoFormat\":{\"classPath\":\"com.example.FeastFeature\"}}}}");
    expectedSparkApplication.setSpec(expectedSparkApplicationSpec);
    verify(api, times(1)).create(expectedSparkApplication);
  }
}
