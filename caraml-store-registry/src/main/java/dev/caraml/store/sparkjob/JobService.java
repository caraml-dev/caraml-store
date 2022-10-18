package dev.caraml.store.sparkjob;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import dev.caraml.store.feature.EntityRepository;
import dev.caraml.store.feature.FeatureTableNotFoundException;
import dev.caraml.store.feature.FeatureTableRepository;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.Job;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.JobStatus;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.JobType;
import dev.caraml.store.sparkjob.adapter.BatchIngestionArgumentAdapter;
import dev.caraml.store.sparkjob.adapter.HistoricalRetrievalArgumentAdapter;
import dev.caraml.store.sparkjob.adapter.ScheduledBatchIngestionArgumentAdapter;
import dev.caraml.store.sparkjob.adapter.StreamIngestionArgumentAdapter;
import dev.caraml.store.sparkjob.crd.ScheduledSparkApplication;
import dev.caraml.store.sparkjob.crd.ScheduledSparkApplicationSpec;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class JobService {

  private final String namespace;
  private final String sparkImage;
  private final DefaultStore defaultStore;
  private final Map<JobType, Map<String, IngestionJobProperties>>
      ingestionJobTemplateByTypeAndStoreName = new HashMap<>();
  private final HistoricalRetrievalJobProperties retrievalJobProperties;
  private final DeltaIngestionDataset deltaIngestionDataset;
  private final EntityRepository entityRepository;
  private final FeatureTableRepository tableRepository;

  private final SparkOperatorApi sparkOperatorApi;

  @Autowired
  public JobService(
      JobServiceConfig config,
      EntityRepository entityRepository,
      FeatureTableRepository tableRepository,
      SparkOperatorApi sparkOperatorApi) {
    namespace = config.getNamespace();
    sparkImage = config.getCommon().sparkImage();
    defaultStore = config.getDefaultStore();
    ingestionJobTemplateByTypeAndStoreName.put(
        JobType.STREAM_INGESTION_JOB,
        config.getStreamIngestion().stream()
            .collect(Collectors.toMap(IngestionJobProperties::store, Function.identity())));
    ingestionJobTemplateByTypeAndStoreName.put(
        JobType.BATCH_INGESTION_JOB,
        config.getBatchIngestion().stream()
            .collect(Collectors.toMap(IngestionJobProperties::store, Function.identity())));
    retrievalJobProperties = config.getHistoricalRetrieval();
    deltaIngestionDataset = config.getDeltaIngestionDataset();
    this.entityRepository = entityRepository;
    this.tableRepository = tableRepository;
    this.sparkOperatorApi = sparkOperatorApi;
  }

  static final String LABEL_PREFIX = "caraml.dev/";
  static final String STORE_LABEL = LABEL_PREFIX + "store";
  static final String JOB_TYPE_LABEL = LABEL_PREFIX + "type";
  static final String FEATURE_TABLE_LABEL = LABEL_PREFIX + "table";
  static final String FEATURE_TABLE_HASH_LABEL = LABEL_PREFIX + "hash";
  static final String PROJECT_LABEL = LABEL_PREFIX + "project";

  static final Integer JOB_ID_LENGTH = 16;
  static final Integer LABEL_CHARACTERS_LIMIT = 63;

  private String getIngestionJobId(JobType jobType, String project, FeatureTableSpec spec) {
    return "caraml-"
        + DigestUtils.md5Hex(
                String.join(
                    "-",
                    jobType.toString(),
                    project,
                    spec.getName(),
                    getDefaultStoreIfAbsent(spec, jobType)))
            .substring(0, JOB_ID_LENGTH)
            .toLowerCase();
  }

  private String getRetrievalJobId() {
    return "caraml-" + RandomStringUtils.randomAlphanumeric(JOB_ID_LENGTH).toLowerCase();
  }

  private Job sparkApplicationToJob(SparkApplication app) {
    Map<String, String> labels = app.getMetadata().getLabels();
    Timestamp startTime =
        Timestamps.fromSeconds(app.getMetadata().getCreationTimestamp().toEpochSecond());

    JobStatus jobStatus = JobStatus.JOB_STATUS_PENDING;
    if (app.getStatus() != null) {
      jobStatus =
          switch (app.getStatus().getApplicationState().getState()) {
            case "COMPLETED" -> JobStatus.JOB_STATUS_DONE;
            case "FAILED" -> JobStatus.JOB_STATUS_ERROR;
            case "RUNNING" -> JobStatus.JOB_STATUS_RUNNING;
            default -> JobStatus.JOB_STATUS_PENDING;
          };
    }
    String tableName = labels.getOrDefault(FEATURE_TABLE_LABEL, "");

    Job.Builder builder =
        Job.newBuilder()
            .setId(app.getMetadata().getName())
            .setStartTime(startTime)
            .setStatus(jobStatus);
    switch (JobType.valueOf(labels.get(JOB_TYPE_LABEL))) {
      case BATCH_INGESTION_JOB -> {
        builder.setBatchIngestion(Job.OfflineToOnlineMeta.newBuilder().setTableName(tableName));
        builder.setType(JobType.BATCH_INGESTION_JOB);
      }
      case STREAM_INGESTION_JOB -> {
        builder.setStreamIngestion(Job.StreamToOnlineMeta.newBuilder().setTableName(tableName));
        builder.setType(JobType.STREAM_INGESTION_JOB);
      }
      case RETRIEVAL_JOB -> {
        builder.setRetrieval(Job.RetrievalJobMeta.newBuilder().build());
        builder.setType(JobType.RETRIEVAL_JOB);
      }
    }

    return builder.build();
  }

  public Job createOrUpdateStreamingIngestionJob(String project, String featureTableName) {
    FeatureTableSpec spec =
        tableRepository
            .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(project, featureTableName)
            .map(ft -> ft.toProto().getSpec())
            .orElseThrow(
                () -> {
                  throw new FeatureTableNotFoundException(project, featureTableName);
                });
    return createOrUpdateStreamingIngestionJob(project, spec);
  }

  private Map<String, String> getEntityToTypeMap(String project, FeatureTableSpec spec) {
    return spec.getEntitiesList().stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                e -> entityRepository.findEntityByNameAndProject_Name(e, project).getType()));
  }

  private Map<String, String> getEntityToTypeMap(String project, List<FeatureTableSpec> specs) {
    return specs.stream()
        .flatMap(spec -> spec.getEntitiesList().stream())
        .distinct()
        .collect(
            Collectors.toMap(
                Function.identity(),
                e -> entityRepository.findEntityByNameAndProject_Name(e, project).getType()));
  }

  private String getDefaultStoreIfAbsent(FeatureTableSpec spec, JobType jobType) {
    String defaultStoreName =
        jobType == JobType.BATCH_INGESTION_JOB ? defaultStore.batch() : defaultStore.stream();
    String onlineStoreName = spec.getOnlineStore().getName();
    return onlineStoreName.isEmpty() || onlineStoreName.equals("unset")
        ? defaultStoreName
        : onlineStoreName;
  }

  private SparkApplicationSpec newSparkApplicationSpecCopy(SparkApplicationSpec spec) {
    SparkApplicationSpec copiedSpec = spec.deepCopy();
    copiedSpec.setImage(sparkImage);
    return copiedSpec;
  }

  public Job createOrUpdateStreamingIngestionJob(String project, FeatureTableSpec spec) {
    Map<String, String> entityNameToType = getEntityToTypeMap(project, spec);
    List<String> arguments =
        new StreamIngestionArgumentAdapter(project, spec, entityNameToType).getArguments();
    return createOrUpdateIngestionJob(JobType.STREAM_INGESTION_JOB, project, spec, arguments);
  }

  public Job createOrUpdateBatchIngestionJob(
      String project,
      String featureTableName,
      Timestamp startTime,
      Timestamp endTime,
      Boolean deltaIngestion)
      throws SparkOperatorApiException {

    FeatureTableSpec spec =
        tableRepository
            .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(featureTableName, project)
            .map(ft -> ft.toProto().getSpec())
            .orElseThrow(
                () -> {
                  throw new FeatureTableNotFoundException(project, featureTableName);
                });
    Map<String, String> entityNameToType = getEntityToTypeMap(project, spec);

    if (deltaIngestion && spec.getBatchSource().getType() == DataSource.SourceType.BATCH_BIGQUERY) {
      FeatureTableSpec.Builder specBuilder = spec.toBuilder();
      specBuilder
          .getBatchSourceBuilder()
          .getBigqueryOptionsBuilder()
          .setTableRef(getDeltaTableRef(project, featureTableName));
      spec = specBuilder.build();
    }

    List<String> arguments =
        new BatchIngestionArgumentAdapter(project, spec, entityNameToType, startTime, endTime)
            .getArguments();
    return createOrUpdateIngestionJob(JobType.BATCH_INGESTION_JOB, project, spec, arguments);
  }

  public void scheduleBatchIngestionJob(
      String project, String featureTableName, String schedule, Integer ingestionTimespan) {
    FeatureTableSpec featureTableSpec =
        tableRepository
            .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(featureTableName, project)
            .map(ft -> ft.toProto().getSpec())
            .orElseThrow(
                () -> {
                  throw new FeatureTableNotFoundException(project, featureTableName);
                });
    String onlineStoreName = getDefaultStoreIfAbsent(featureTableSpec, JobType.BATCH_INGESTION_JOB);
    IngestionJobProperties batchIngestionJobTemplate =
        ingestionJobTemplateByTypeAndStoreName
            .get(JobType.BATCH_INGESTION_JOB)
            .get(onlineStoreName);
    if (batchIngestionJobTemplate == null) {
      throw new IllegalArgumentException(
          String.format("Job properties not found for store name: %s", onlineStoreName));
    }

    String ingestionJobId =
        getIngestionJobId(JobType.BATCH_INGESTION_JOB, project, featureTableSpec);
    Optional<ScheduledSparkApplication> existingScheduledApplication =
        sparkOperatorApi.getScheduledSparkApplication(namespace, ingestionJobId);
    ScheduledSparkApplication scheduledSparkApplication =
        existingScheduledApplication.orElseGet(
            () -> {
              ScheduledSparkApplication scheduledApp = new ScheduledSparkApplication();
              scheduledApp.setMetadata(new V1ObjectMeta());
              scheduledApp.getMetadata().setName(ingestionJobId);
              scheduledApp.getMetadata().setNamespace(namespace);
              scheduledApp.addLabels(
                  Map.of(
                      JOB_TYPE_LABEL,
                      JobType.BATCH_INGESTION_JOB.toString(),
                      STORE_LABEL,
                      onlineStoreName,
                      PROJECT_LABEL,
                      project,
                      FEATURE_TABLE_HASH_LABEL,
                      generateProjectTableHash(project, featureTableName),
                      FEATURE_TABLE_LABEL,
                      StringUtils.truncate(featureTableName, LABEL_CHARACTERS_LIMIT)));
              return scheduledApp;
            });
    SparkApplicationSpec appSpec =
        newSparkApplicationSpecCopy(batchIngestionJobTemplate.sparkApplicationSpec());
    Map<String, String> entityNameToType = getEntityToTypeMap(project, featureTableSpec);
    List<String> arguments =
        new ScheduledBatchIngestionArgumentAdapter(
                project, featureTableSpec, entityNameToType, ingestionTimespan)
            .getArguments();
    appSpec.addArguments(arguments);
    ScheduledSparkApplicationSpec scheduledAppSpec =
        new ScheduledSparkApplicationSpec(schedule, appSpec);
    scheduledSparkApplication.setSpec(scheduledAppSpec);
    if (existingScheduledApplication.isPresent()) {
      sparkOperatorApi.update(scheduledSparkApplication);
    } else {
      sparkOperatorApi.create(scheduledSparkApplication);
    }
  }

  private Job createOrUpdateIngestionJob(
      JobType jobType, String project, FeatureTableSpec spec, List<String> additionalArguments) {

    Map<String, IngestionJobProperties> jobTemplateByStoreName =
        ingestionJobTemplateByTypeAndStoreName.get(jobType);
    if (jobTemplateByStoreName == null) {
      throw new IllegalArgumentException(
          String.format("Job properties not found for job type: %s", jobType.toString()));
    }
    String onlineStoreName = getDefaultStoreIfAbsent(spec, jobType);
    IngestionJobProperties jobProperties = jobTemplateByStoreName.get(onlineStoreName);
    if (jobProperties == null) {
      throw new IllegalArgumentException(
          String.format("Job properties not found for store name: %s", onlineStoreName));
    }

    String ingestionJobId = getIngestionJobId(jobType, project, spec);
    Optional<SparkApplication> existingApplication =
        sparkOperatorApi.getSparkApplication(namespace, ingestionJobId);
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
                      STORE_LABEL,
                      onlineStoreName,
                      PROJECT_LABEL,
                      project,
                      FEATURE_TABLE_HASH_LABEL,
                      generateProjectTableHash(project, spec.getName()),
                      FEATURE_TABLE_LABEL,
                      StringUtils.truncate(spec.getName(), LABEL_CHARACTERS_LIMIT)));
              return app;
            });
    sparkApplication.setSpec(newSparkApplicationSpecCopy(jobProperties.sparkApplicationSpec()));
    sparkApplication.getSpec().addArguments(additionalArguments);

    SparkApplication updatedApplication =
        existingApplication.isPresent()
            ? sparkOperatorApi.update(sparkApplication)
            : sparkOperatorApi.create(sparkApplication);
    return sparkApplicationToJob(updatedApplication);
  }

  public Job createRetrievalJob(
      String project,
      List<String> featureRefs,
      DataSource entitySource,
      String outputFormat,
      String outputUri) {
    if (retrievalJobProperties == null || retrievalJobProperties.sparkApplicationSpec() == null) {
      throw new IllegalArgumentException(
          "Historical retrieval job properties have not been configured");
    }
    Map<String, List<String>> featuresGroupedByFeatureTable =
        featureRefs.stream()
            .map(ref -> ref.split(":"))
            .collect(
                Collectors.toMap(
                    splitRef -> splitRef[0],
                    splitRef -> List.of(splitRef[1]),
                    (left, right) -> Stream.concat(left.stream(), right.stream()).toList()));
    List<FeatureTableSpec> featureTableSpecs =
        featuresGroupedByFeatureTable.entrySet().stream()
            .map(
                es -> {
                  FeatureTableSpec featureTableSpec =
                      tableRepository
                          .findFeatureTableByNameAndProject_NameAndIsDeletedFalse(
                              es.getKey(), project)
                          .map(ft -> ft.toProto().getSpec())
                          .orElseThrow(
                              () -> new FeatureTableNotFoundException(project, es.getKey()));
                  List<FeatureSpec> filteredFeatures =
                      featureTableSpec.getFeaturesList().stream()
                          .filter(f -> es.getValue().contains(f.getName()))
                          .toList();
                  return featureTableSpec.toBuilder()
                      .clearFeatures()
                      .addAllFeatures(filteredFeatures)
                      .build();
                })
            .toList();

    SparkApplication app = new SparkApplication();
    app.setMetadata(new V1ObjectMeta());
    app.getMetadata().setName(getRetrievalJobId());
    app.getMetadata().setNamespace(namespace);
    app.addLabels(Map.of(JOB_TYPE_LABEL, JobType.RETRIEVAL_JOB.toString(), PROJECT_LABEL, project));
    app.setSpec(newSparkApplicationSpecCopy(retrievalJobProperties.sparkApplicationSpec()));
    Map<String, String> entityNameToType = getEntityToTypeMap(project, featureTableSpecs);
    List<String> arguments =
        new HistoricalRetrievalArgumentAdapter(
                project, featureTableSpecs, entityNameToType, entitySource, outputFormat, outputUri)
            .getArguments();
    app.getSpec().addArguments(arguments);

    return sparkApplicationToJob(sparkOperatorApi.create(app));
  }

  public List<Job> listJobs(Boolean includeTerminated, String project, String tableName) {
    Stream<String> equalitySelectors =
        Map.of(
                PROJECT_LABEL,
                project,
                FEATURE_TABLE_HASH_LABEL,
                generateProjectTableHash(project, tableName))
            .entrySet()
            .stream()
            .filter(es -> !es.getValue().isEmpty())
            .map(es -> String.format("%s=%s", es.getKey(), es.getValue()));
    String jobSets =
        Stream.of(JobType.BATCH_INGESTION_JOB, JobType.STREAM_INGESTION_JOB, JobType.RETRIEVAL_JOB)
            .map(Enum::toString)
            .collect(Collectors.joining(","));
    Stream<String> setSelectors = Stream.of(String.format("%s in (%s)", JOB_TYPE_LABEL, jobSets));
    String labelSelectors =
        Stream.concat(equalitySelectors, setSelectors).collect(Collectors.joining(","));
    Stream<Job> jobStream =
        sparkOperatorApi.list(namespace, labelSelectors).stream().map(this::sparkApplicationToJob);
    if (!includeTerminated) {
      jobStream = jobStream.filter(job -> job.getStatus() == JobStatus.JOB_STATUS_RUNNING);
    }
    return jobStream.toList();
  }

  public Optional<Job> getJob(String id) {
    return sparkOperatorApi.getSparkApplication(namespace, id).map(this::sparkApplicationToJob);
  }

  private String generateProjectTableHash(String project, String tableName) {
    return DigestUtils.md5Hex(String.format("%s:%s", project, tableName));
  }

  private String getDeltaTableRef(String projectName, String featureTableName) {
    return String.format(
        "%s:%s.%s_%s_delta",
        deltaIngestionDataset.project(),
        deltaIngestionDataset.dataset(),
        projectName,
        featureTableName);
  }
}
