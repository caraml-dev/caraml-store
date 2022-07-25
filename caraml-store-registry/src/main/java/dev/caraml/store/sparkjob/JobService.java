package dev.caraml.store.sparkjob;

import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.caraml.store.feature.EntityRepository;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobService {

  private final Map<JobType, Map<String, JobServiceConfig.SparkJobProperties>>
      jobConfigByTypeAndStoreName = new HashMap<>();
  private final EntityRepository entityRepository;

  private final SparkOperatorApi sparkOperatorApi;

  @Autowired
  public JobService(
      JobServiceConfig config,
      EntityRepository entityRepository,
      SparkOperatorApi sparkOperatorApi) {
    this.jobConfigByTypeAndStoreName.put(
        JobType.STREAM_TO_ONLINE_JOB,
        config.getStreamingJobs().stream()
            .collect(
                Collectors.toMap(JobServiceConfig.SparkJobProperties::store, Function.identity())));
    this.entityRepository = entityRepository;
    this.sparkOperatorApi = sparkOperatorApi;
  }

  enum JobType {
    STREAM_TO_ONLINE_JOB,
    OFFLINE_TO_ONLINE_JOB,
    HISTORICAL_RETRIEVAL_JOB,
  }

  static final String LABEL_PREFIX = "caraml.dev/";
  static final String JOB_TYPE_LABEL = LABEL_PREFIX + "type";
  static final String FEATURE_TABLE_LABEL = LABEL_PREFIX + "table";
  static final String PROJECT_LABEL = LABEL_PREFIX + "project";

  @Data
  static class Field {
    private final String name;
    private final String type;
  }

  @Data
  static class FeatureTableArgument {
    private final String project;
    private final String name;
    private final Map<String, String> labels;
    private final Integer maxAge;
    private final List<Field> entities;
    private final List<Field> features;
  }

  private String getIngestionJobId(JobType jobType, String project, FeatureTableSpec spec) {
    return "caraml-"
        + DigestUtils.md5Hex(
                String.join(
                    "-",
                    jobType.toString(),
                    project,
                    spec.getName(),
                    spec.getOnlineStore().getName()))
            .toLowerCase();
  }

  private String featureTableSpecToArgument(String project, FeatureTableSpec spec) {
    FeatureTableArgument arg =
        new FeatureTableArgument(
            project,
            spec.getName(),
            spec.getLabelsMap(),
            Math.toIntExact(spec.getMaxAge().getSeconds()),
            spec.getEntitiesList().stream()
                .map(
                    e ->
                        new Field(
                            e,
                            entityRepository.findEntityByNameAndProject_Name(e, project).getType()))
                .collect(Collectors.toList()),
            spec.getFeaturesList().stream()
                .map(f -> new Field(f.getName(), f.getValueType().toString()))
                .collect(Collectors.toList()));
    return new Gson().toJson(arg);
  }

  void augmentIngestionJob(
      JobType jobType, SparkApplication sparkApplication, String project, FeatureTableSpec spec)
      throws InvalidProtocolBufferException {
    if (sparkApplication.getMetadata().getLabels() == null) {
      sparkApplication.getMetadata().setLabels(new HashMap<>());
    }
    Map<String, String> labels = sparkApplication.getMetadata().getLabels();
    labels.put(JOB_TYPE_LABEL, jobType.toString());
    labels.put(PROJECT_LABEL, project);
    labels.put(FEATURE_TABLE_LABEL, spec.getName());
    sparkApplication
        .getSpec()
        .addArgument("feature-table", featureTableSpecToArgument(project, spec));
    DataSource datasource =
        jobType == JobType.STREAM_TO_ONLINE_JOB ? spec.getStreamSource() : spec.getBatchSource();
    sparkApplication
        .getSpec()
        .addArgument(
            "source", JsonFormat.printer().omittingInsignificantWhitespace().print(datasource));
  }

  public void createOrUpdateStreamingIngestionJob(String project, FeatureTableSpec spec)
      throws InvalidProtocolBufferException, SparkOperatorApiException {
    createOrUpdateIngestionJob(JobType.STREAM_TO_ONLINE_JOB, project, spec);
  }

  private void createOrUpdateIngestionJob(JobType jobType, String project, FeatureTableSpec spec)
      throws SparkOperatorApiException, InvalidProtocolBufferException {

    Map<String, JobServiceConfig.SparkJobProperties> jobConfigByStoreName =
        jobConfigByTypeAndStoreName.get(jobType);
    if (jobConfigByStoreName == null) {
      throw new SparkOperatorApiException(
          String.format("Job properties not found for job type: %s", jobType.toString()));
    }
    JobServiceConfig.SparkJobProperties jobProperties =
        jobConfigByStoreName.get(spec.getOnlineStore().getName());
    if (jobProperties == null) {
      throw new SparkOperatorApiException(
          String.format(
              "Job properties not found for store name: %s", spec.getOnlineStore().getName()));
    }
    String namespace = jobProperties.namespace();

    String ingestionJobId = getIngestionJobId(jobType, project, spec);
    Optional<SparkApplication> existingApplication =
        sparkOperatorApi.get(namespace, ingestionJobId);
    SparkApplication sparkApplication =
        existingApplication.orElseGet(
            () -> {
              SparkApplication app = new SparkApplication();
              app.setMetadata(new V1ObjectMeta());
              app.getMetadata().setName(ingestionJobId);
              app.getMetadata().setNamespace(namespace);
              return app;
            });
    sparkApplication.setSpec(jobProperties.sparkApplicationSpec());
    augmentIngestionJob(jobType, sparkApplication, project, spec);

    if (existingApplication.isPresent()) {
      sparkOperatorApi.update(sparkApplication);
    } else {
      sparkOperatorApi.create(sparkApplication);
    }
  }
}
