package dev.caraml.store.api;

import dev.caraml.store.feature.Project;
import dev.caraml.store.feature.RegistryConfig;
import dev.caraml.store.feature.RegistryService;
import dev.caraml.store.protobuf.core.CoreServiceGrpc;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyEntityRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyEntityResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyFeatureTableResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ArchiveOnlineStoreRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ArchiveOnlineStoreResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ArchiveProjectRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ArchiveProjectResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.CreateProjectRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.CreateProjectResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.DeleteFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.DeleteFeatureTableResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetEntityRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetEntityResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeastCoreVersionRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeastCoreVersionResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeatureTableResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetOnlineStoreRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetOnlineStoreResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListEntitiesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListEntitiesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeaturesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeaturesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListOnlineStoresRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListOnlineStoresResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListProjectsRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListProjectsResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.RegisterOnlineStoreRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.RegisterOnlineStoreResponse;
import dev.caraml.store.protobuf.core.EntityProto.EntitySpec;
import dev.caraml.store.sparkjob.JobService;
import dev.caraml.store.sparkjob.SparkOperatorApiException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;

@Slf4j
@GrpcService
public class RegistryGrpcServiceImpl extends CoreServiceGrpc.CoreServiceImplBase {

  private final String version;
  private final RegistryService registryService;
  private final JobService jobService;
  private final RegistryConfig config;

  @Autowired
  public RegistryGrpcServiceImpl(
      RegistryService registryService,
      JobService jobService,
      RegistryConfig config,
      BuildProperties buildProperties) {
    this.registryService = registryService;
    this.jobService = jobService;
    this.version = buildProperties.getVersion();
    this.config = config;
  }

  @Override
  public void getFeastCoreVersion(
      GetFeastCoreVersionRequest request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    GetFeastCoreVersionResponse response =
        GetFeastCoreVersionResponse.newBuilder().setVersion(getVersion()).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getEntity(
      GetEntityRequest request, StreamObserver<GetEntityResponse> responseObserver) {
    GetEntityResponse response = registryService.getEntity(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /** Retrieve a list of features */
  @Override
  public void listFeatures(
      ListFeaturesRequest request, StreamObserver<ListFeaturesResponse> responseObserver) {
    ListFeaturesResponse response = registryService.listFeatures(request.getFilter());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /** Retrieve a list of entities */
  @Override
  public void listEntities(
      ListEntitiesRequest request, StreamObserver<ListEntitiesResponse> responseObserver) {
    ListEntitiesResponse response = registryService.listEntities(request.getFilter());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /* Registers an entity */
  @Override
  public void applyEntity(
      ApplyEntityRequest request, StreamObserver<ApplyEntityResponse> responseObserver) {

    String projectId;

    try {
      EntitySpec spec = request.getSpec();
      projectId = request.getProject();
      ApplyEntityResponse response = registryService.applyEntity(spec, projectId);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          "Unable to persist this entity due to a constraint violation. Please ensure that"
              + " field names are unique within the project namespace: ",
          e);
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void createProject(
      CreateProjectRequest request, StreamObserver<CreateProjectResponse> responseObserver) {
    registryService.createProject(request.getName());
    responseObserver.onNext(CreateProjectResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void archiveProject(
      ArchiveProjectRequest request, StreamObserver<ArchiveProjectResponse> responseObserver) {
    String projectId;
    projectId = request.getName();
    registryService.archiveProject(projectId);
    responseObserver.onNext(ArchiveProjectResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void listProjects(
      ListProjectsRequest request, StreamObserver<ListProjectsResponse> responseObserver) {
    List<Project> projects = registryService.listProjects();
    responseObserver.onNext(
        ListProjectsResponse.newBuilder()
            .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void applyFeatureTable(
      ApplyFeatureTableRequest request,
      StreamObserver<ApplyFeatureTableResponse> responseObserver) {

    String projectName = RegistryService.resolveProjectName(request.getProject());
    String tableName = request.getTableSpec().getName();
    try {
      ApplyFeatureTableResponse response = registryService.applyFeatureTable(request);
      if (request.getTableSpec().hasStreamSource() && config.getSyncIngestionJobOnSpecUpdate()) {
        jobService.createOrUpdateStreamingIngestionJob(
            request.getProject(), request.getTableSpec());
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Unable to apply Feature Table due to a conflict: "
                  + "Ensure that name is unique within Project: (name: %s, project: %s)",
              tableName, projectName));
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (SparkOperatorApiException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: feature spec was applied but streaming job creation failed: (name: %s, project: %s)",
              tableName, projectName));
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listFeatureTables(
      ListFeatureTablesRequest request,
      StreamObserver<ListFeatureTablesResponse> responseObserver) {

    ListFeatureTablesResponse response = registryService.listFeatureTables(request.getFilter());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getFeatureTable(
      GetFeatureTableRequest request, StreamObserver<GetFeatureTableResponse> responseObserver) {
    GetFeatureTableResponse response = registryService.getFeatureTable(request);

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteFeatureTable(
      DeleteFeatureTableRequest request,
      StreamObserver<DeleteFeatureTableResponse> responseObserver) {
    registryService.deleteFeatureTable(request);

    responseObserver.onNext(DeleteFeatureTableResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void listOnlineStores(
      ListOnlineStoresRequest request, StreamObserver<ListOnlineStoresResponse> responseObserver) {
    ListOnlineStoresResponse response = registryService.listOnlineStores();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineStore(
      GetOnlineStoreRequest request, StreamObserver<GetOnlineStoreResponse> responseObserver) {
    GetOnlineStoreResponse response = registryService.getOnlineStore(request.getName());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void registerOnlineStore(
      RegisterOnlineStoreRequest request,
      StreamObserver<RegisterOnlineStoreResponse> responseObserver) {
    RegisterOnlineStoreResponse response = registryService.registerOnlineStore(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void archiveOnlineStore(
      ArchiveOnlineStoreRequest request,
      StreamObserver<ArchiveOnlineStoreResponse> responseObserver) {
    ArchiveOnlineStoreResponse response = registryService.archiveOnlineStore(request.getName());
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  public String getVersion() {
    return version;
  }
}
