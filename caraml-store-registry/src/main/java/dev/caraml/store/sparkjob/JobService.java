package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import dev.caraml.store.feature.EntityRepository;
import dev.caraml.store.feature.FeatureTableRepository;
import dev.caraml.store.feature.SpecNotFoundException;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobService {

  private final Map<JobType, Map<String, JobServiceConfig.IngestionJobProperties>>
      ingestionJobsByTypeAndStoreName = new HashMap<>();
  private final EntityRepository entityRepository;
  private final FeatureTableRepository tableRepository;

  private final SparkOperatorApi sparkOperatorApi;

  @Autowired
  public JobService(
      JobServiceConfig config,
      EntityRepository entityRepository,
      FeatureTableRepository tableRepository,
      SparkOperatorApi sparkOperatorApi) {
    this.ingestionJobsByTypeAndStoreName.put(
        JobType.STREAM_TO_ONLINE_JOB,
        config.getStreamIngestion().stream()
            .collect(
                Collectors.toMap(
                    JobServiceConfig.IngestionJobProperties::store, Function.identity())));
    this.ingestionJobsByTypeAndStoreName.put(
        JobType.OFFLINE_TO_ONLINE_JOB,
        config.getBatchIngestion().stream()
            .collect(
                Collectors.toMap(
                    JobServiceConfig.IngestionJobProperties::store, Function.identity())));
    this.entityRepository = entityRepository;
    this.tableRepository = tableRepository;
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

  record Field(String name, String type) {}

  record FeatureTableArgument(
      String project,
      String name,
      Map<String, String> labels,
      Integer maxAge,
      List<Field> entities,
      List<Field> features) {}

  record FlagArgument(String name, String value) {}

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

  private String featureTableSpecAsJsonString(String project, FeatureTableSpec spec)
      throws JsonProcessingException {
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
    return new ObjectMapper().writeValueAsString(arg);
  }

  private List<FlagArgument> featureTableSpecAsArguments(
      String project, FeatureTableSpec featureTableSpec, DataSource datasource)
      throws InvalidProtocolBufferException, JsonProcessingException {
    return List.of(
        new FlagArgument("feature-table", featureTableSpecAsJsonString(project, featureTableSpec)),
        new FlagArgument(
            "source", JsonFormat.printer().omittingInsignificantWhitespace().print(datasource)));
  }

  private List<FlagArgument> ingestionTimeSpanAsArguments(Timestamp startTime, Timestamp endTime) {
    return List.of(
        new FlagArgument("start", Timestamps.toString(startTime)),
        new FlagArgument("end", Timestamps.toString(endTime)));
  }

  public SparkApplication createOrUpdateStreamingIngestionJob(
      String project, String featureTableName)
      throws InvalidProtocolBufferException, SparkOperatorApiException {
    FeatureTableSpec spec =
        tableRepository
            .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(project, featureTableName)
            .map(ft -> ft.toProto().getSpec())
            .orElseThrow(
                () -> {
                  throw new SpecNotFoundException(
                      String.format(
                          "No such Feature Table: (project: %s, name: %s)",
                          project, featureTableName));
                });
    return createOrUpdateStreamingIngestionJob(project, spec);
  }

  public SparkApplication createOrUpdateStreamingIngestionJob(String project, FeatureTableSpec spec)
      throws InvalidProtocolBufferException, SparkOperatorApiException {
    try {
      List<FlagArgument> additionalArguments =
          featureTableSpecAsArguments(project, spec, spec.getStreamSource());
      return createOrUpdateIngestionJob(
          JobType.STREAM_TO_ONLINE_JOB, project, spec, additionalArguments);
    } catch (InvalidProtocolBufferException | JsonProcessingException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  public SparkApplication createOrUpdateBatchIngestionJob(
      String project, String featureTableName, Timestamp startTime, Timestamp endTime)
      throws SparkOperatorApiException {
    FeatureTableSpec spec =
        tableRepository
            .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(featureTableName, project)
            .map(ft -> ft.toProto().getSpec())
            .orElseThrow(
                () -> {
                  throw new SpecNotFoundException(
                      String.format(
                          "No such Feature Table: (project: %s, name: %s)",
                          project, featureTableName));
                });

    try {
      List<FlagArgument> additionalArguments =
          Stream.concat(
                  featureTableSpecAsArguments(project, spec, spec.getBatchSource()).stream(),
                  ingestionTimeSpanAsArguments(startTime, endTime).stream())
              .toList();
      return createOrUpdateIngestionJob(
          JobType.OFFLINE_TO_ONLINE_JOB, project, spec, additionalArguments);
    } catch (InvalidProtocolBufferException | JsonProcessingException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  private SparkApplication createOrUpdateIngestionJob(
      JobType jobType,
      String project,
      FeatureTableSpec spec,
      List<FlagArgument> additionalArguments)
      throws SparkOperatorApiException, InvalidProtocolBufferException {

    Map<String, JobServiceConfig.IngestionJobProperties> jobConfigByStoreName =
        ingestionJobsByTypeAndStoreName.get(jobType);
    if (jobConfigByStoreName == null) {
      throw new IllegalArgumentException(
          String.format("Job properties not found for job type: %s", jobType.toString()));
    }
    JobServiceConfig.IngestionJobProperties jobProperties =
        jobConfigByStoreName.get(spec.getOnlineStore().getName());
    if (jobProperties == null) {
      throw new IllegalArgumentException(
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
              app.addLabels(
                  Map.of(
                      JOB_TYPE_LABEL,
                      jobType.toString(),
                      PROJECT_LABEL,
                      project,
                      FEATURE_TABLE_LABEL,
                      spec.getName()));
              return app;
            });
    sparkApplication.setSpec(jobProperties.sparkApplicationSpec());
    additionalArguments.forEach(
        flagArg -> sparkApplication.getSpec().addArgument(flagArg.name, flagArg.value));

    if (existingApplication.isPresent()) {
      sparkOperatorApi.update(sparkApplication);
    } else {
      sparkOperatorApi.create(sparkApplication);
    }
    return sparkApplication;
  }
}
