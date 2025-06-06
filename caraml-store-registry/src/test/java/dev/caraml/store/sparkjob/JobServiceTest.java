package dev.caraml.store.sparkjob;

import static org.mockito.ArgumentMatchers.any;
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
import dev.caraml.store.sparkjob.crd.*;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.OffsetDateTime;
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
  @Mock private ProjectContextProvider projectContextProvider;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    entityRepository = mock(EntityRepository.class);
    api = mock(SparkOperatorApi.class);
    when(api.create(any(SparkApplication.class)))
        .thenAnswer(
            invocation -> {
              SparkApplication input = invocation.getArgument(0, SparkApplication.class);
              SparkApplication appWithStatus = new SparkApplication();
              appWithStatus.setSpec(input.getSpec());
              appWithStatus.setKind(input.getKind());
              V1ObjectMeta metadata = new V1ObjectMeta();
              metadata.setName(input.getMetadata().getName());
              metadata.setLabels(input.getMetadata().getLabels());
              metadata.setCreationTimestamp(OffsetDateTime.now());
              appWithStatus.setMetadata(metadata);
              SparkApplicationStatus status = new SparkApplicationStatus();
              status.setApplicationState(new SparkApplicationState("SUBMITTED"));
              appWithStatus.setStatus(status);
              return appWithStatus;
            });
    when(projectContextProvider.getContext("project")).thenReturn(Map.of("team", "some-team"));
  }

  @Test
  public void shouldCreateBatchIngestionJob() throws IOException, ParseException {
    List<IngestionJobTemplate> jobs = new ArrayList<>();
    JobServiceConfig properties = new JobServiceConfig();
    properties.setNamespace("spark-operator");
    properties.setBatchIngestion(jobs);
    properties.setCommon(new CommonJobProperties("sparkImage:latest"));
    properties.setDefaultStore(new DefaultStore("store", "store"));
    properties.setDeltaIngestionDataset(new DeltaIngestionDataset("bq-project", "bq-dataset"));
    IngestionJobTemplate batchJobProperty =
        new IngestionJobTemplate("store", new SparkApplicationSpec());
    jobs.add(batchJobProperty);
    JobService jobservice =
        new JobService(properties, entityRepository, tableRepository, api, projectContextProvider);
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
    Long defaultMaxAgeSeconds = 100L;
    FeatureTable featureTable =
        FeatureTable.fromProto(project, spec, entityRepository, defaultMaxAgeSeconds);
    when(tableRepository.findFeatureTableByNameAndProject_NameAndIsDeletedFalse(
            "batch_feature_table", project))
        .thenReturn(Optional.of(featureTable));
    when(tableRepository.findAllByProject_Name(project)).thenReturn(List.of(featureTable));
    SparkApplication expectedSparkApplication = new SparkApplication();
    V1ObjectMeta expectedMetadata = new V1ObjectMeta();
    expectedMetadata.setLabels(
        Map.of(
            "caraml.dev/table", "batch_feature_table",
            "caraml.dev/store", "store",
            "caraml.dev/project", "project",
            "caraml.dev/hash", "b52c978f4c9a1713f45dce2a1535b07a",
            "caraml.dev/type", "BATCH_INGESTION_JOB"));
    expectedMetadata.setNamespace("spark-operator");
    expectedMetadata.setName("caraml-5b41d97ec1180f18");
    expectedSparkApplication.setMetadata(expectedMetadata);
    SparkApplicationSpec expectedSparkApplicationSpec = new SparkApplicationSpec();
    expectedSparkApplicationSpec.setImage("sparkImage:latest");
    expectedSparkApplicationSpec.addArguments(
        List.of(
            "--feature-table",
            "{\"project\":\"project\",\"name\":\"batch_feature_table\",\"labels\":{},\"maxAge\":100,\"entities\":[{\"name\":\"entity1\",\"type\":\"STRING\"}],\"features\":[{\"name\":\"feature1\",\"type\":\"INT64\"}]}",
            "--source",
            "{\"bq\":{\"project\":\"project\",\"dataset\":\"dataset\",\"table\":\"table\",\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}",
            "--start",
            "2022-08-01T01:00:00Z",
            "--end",
            "2022-08-02T01:00:00Z",
            "--entity-max-age",
            "100"));
    expectedSparkApplication.setSpec(expectedSparkApplicationSpec);
    jobservice.createOrUpdateBatchIngestionJob(
        project, "batch_feature_table", ingestionStart, ingestionEnd, false);
    verify(api, times(1)).create(expectedSparkApplication);

    // delta ingestion bq source override test
    expectedSparkApplicationSpec.setArguments(
        List.of(
            "--feature-table",
            "{\"project\":\"project\",\"name\":\"batch_feature_table\",\"labels\":{},\"maxAge\":100,\"entities\":[{\"name\":\"entity1\",\"type\":\"STRING\"}],\"features\":[{\"name\":\"feature1\",\"type\":\"INT64\"}]}",
            "--source",
            "{\"bq\":{\"project\":\"bq-project\",\"dataset\":\"bq-dataset\",\"table\":\"project_batch_feature_table_delta\",\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}",
            "--start",
            "2022-08-01T01:00:00Z",
            "--end",
            "2022-08-02T01:00:00Z",
            "--entity-max-age",
            "100"));
    jobservice.createOrUpdateBatchIngestionJob(
        project, "batch_feature_table", ingestionStart, ingestionEnd, true);
    verify(api, times(1)).create(expectedSparkApplication);
  }

  @Test
  public void shouldCreateStreamingJob() throws IOException, SparkOperatorApiException {
    List<IngestionJobTemplate> jobs = new ArrayList<>();
    JobServiceConfig properties = new JobServiceConfig();
    properties.setNamespace("spark-operator");
    properties.setStreamIngestion(jobs);
    properties.setCommon(new CommonJobProperties("sparkImage:latest"));
    properties.setDefaultStore(new DefaultStore("store", "store"));
    SparkApplicationSpec templateSparkApplicationSpec = new SparkApplicationSpec();
    SparkDriverSpec templateDriverSpec = new SparkDriverSpec();
    SparkExecutorSpec templateExecutorSpec = new SparkExecutorSpec();
    templateDriverSpec.setCores(1);
    templateDriverSpec.setMemory("1g");
    templateDriverSpec.setMemoryOverhead("250m");
    templateDriverSpec.setLabels(Map.of("team", "${team}"));
    templateExecutorSpec.setCores(2);
    templateExecutorSpec.setCoreRequest("100m");
    templateExecutorSpec.setCoreLimit("200m");
    templateExecutorSpec.setMemory("2g");
    templateSparkApplicationSpec.setDriver(templateDriverSpec);
    templateSparkApplicationSpec.setExecutor(templateExecutorSpec);
    IngestionJobTemplate streamJobProperty =
        new IngestionJobTemplate("store", templateSparkApplicationSpec);
    jobs.add(streamJobProperty);
    JobService jobservice =
        new JobService(properties, entityRepository, tableRepository, api, projectContextProvider);
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
    Long defaultMaxAgeSeconds = 100L;
    List<FeatureTable> featureTables =
        List.of(FeatureTable.fromProto(project, spec, entityRepository, defaultMaxAgeSeconds));
    when(tableRepository.findAllByProject_Name(project)).thenReturn(featureTables);
    jobservice.createOrUpdateStreamingIngestionJob(project, spec);
    SparkApplication expectedSparkApplication = new SparkApplication();
    V1ObjectMeta expectedMetadata = new V1ObjectMeta();
    expectedMetadata.setLabels(
        Map.of(
            "caraml.dev/table", "streaming_feature_table",
            "caraml.dev/hash", "20fef0730bf3206ad797b786e68abd5a",
            "caraml.dev/store", "store",
            "caraml.dev/project", "project",
            "caraml.dev/type", "STREAM_INGESTION_JOB"));
    expectedMetadata.setNamespace("spark-operator");
    expectedMetadata.setName("caraml-f6c31d965f86ccf2");
    expectedSparkApplication.setMetadata(expectedMetadata);
    SparkApplicationSpec expectedSparkApplicationSpec = new SparkApplicationSpec();
    SparkDriverSpec expectedDriverSpec = new SparkDriverSpec();
    expectedDriverSpec.setCores(2);
    expectedDriverSpec.setMemory("1g");
    expectedDriverSpec.setMemoryOverhead("250m");
    expectedDriverSpec.setLabels(Map.of("team", "some-team"));
    expectedSparkApplicationSpec.setDriver(expectedDriverSpec);
    SparkExecutorSpec expectedExecutorSpec = new SparkExecutorSpec();
    expectedExecutorSpec.setCores(2);
    expectedExecutorSpec.setCoreRequest("100m");
    expectedExecutorSpec.setCoreLimit("200m");
    expectedExecutorSpec.setMemory("3g");
    expectedSparkApplicationSpec.setExecutor(expectedExecutorSpec);
    expectedSparkApplicationSpec.setImage("sparkImage:latest");
    expectedSparkApplicationSpec.addArguments(
        List.of(
            "--feature-table",
            "{\"project\":\"project\",\"name\":\"streaming_feature_table\",\"labels\":{},\"maxAge\":100,\"entities\":[{\"name\":\"entity1\",\"type\":\"STRING\"}],\"features\":[{\"name\":\"feature1\",\"type\":\"FLOAT\"}]}",
            "--source",
            "{\"kafka\":{\"bootstrapServers\":\"kafka:9102\",\"topic\":\"topic\",\"format\":{\"classPath\":\"com.example.FeastFeature\",\"jsonClass\":\"ProtoFormat\"},\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}",
            "--entity-max-age",
            "100"));
    expectedSparkApplication.setSpec(expectedSparkApplicationSpec);
    verify(api, times(1)).create(expectedSparkApplication);
  }
}
