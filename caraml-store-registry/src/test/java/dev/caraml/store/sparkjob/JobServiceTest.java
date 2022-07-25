package dev.caraml.store.sparkjob;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.util.JsonFormat;
import dev.caraml.store.feature.Entity;
import dev.caraml.store.feature.EntityRepository;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import dev.caraml.store.protobuf.types.ValueProto;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JobServiceTest {

  @Mock private EntityRepository entityRepository;

  @Mock private SparkOperatorApi api;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    entityRepository = mock(EntityRepository.class);
    api = mock(SparkOperatorApi.class);
  }

  @Test
  public void shouldCreateStreamingJob() throws IOException, SparkOperatorApiException {
    List<JobServiceConfig.SparkJobProperties> jobs = new ArrayList<>();
    JobServiceConfig properties = new JobServiceConfig();
    properties.setStreamingJobs(jobs);
    JobServiceConfig.SparkJobProperties streamJobProperty =
        new JobServiceConfig.SparkJobProperties(
            "store", "spark-operator", new SparkApplicationSpec());
    jobs.add(streamJobProperty);
    JobService jobservice = new JobService(properties, entityRepository, api);
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
