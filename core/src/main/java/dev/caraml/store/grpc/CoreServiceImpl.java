package dev.caraml.store.grpc;

import dev.caraml.store.exception.RetrievalException;
import dev.caraml.store.grpc.interceptors.MonitoringInterceptor;
import dev.caraml.store.model.Project;
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
import dev.caraml.store.service.ProjectService;
import dev.caraml.store.service.SpecService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;

@Slf4j
@GrpcService(interceptors = {MonitoringInterceptor.class})
public class CoreServiceImpl extends CoreServiceGrpc.CoreServiceImplBase {

  private final String version;
  private final SpecService specService;
  private final ProjectService projectService;

  @Autowired
  public CoreServiceImpl(
      SpecService specService, ProjectService projectService, BuildProperties buildProperties) {
    this.specService = specService;
    this.projectService = projectService;
    this.version = buildProperties.getVersion();
  }

  @Override
  public void getFeastCoreVersion(
      GetFeastCoreVersionRequest request,
      StreamObserver<GetFeastCoreVersionResponse> responseObserver) {
    try {
      GetFeastCoreVersionResponse response =
          GetFeastCoreVersionResponse.newBuilder().setVersion(getVersion()).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException | StatusRuntimeException e) {
      log.error("Could not determine Feast Core version: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getEntity(
      GetEntityRequest request, StreamObserver<GetEntityResponse> responseObserver) {
    try {
      GetEntityResponse response = specService.getEntity(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (RetrievalException e) {
      log.error("Unable to fetch entity requested in GetEntity method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to GetEntity method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in GetEntity method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /** Retrieve a list of features */
  @Override
  public void listFeatures(
      ListFeaturesRequest request, StreamObserver<ListFeaturesResponse> responseObserver) {
    try {
      ListFeaturesResponse response = specService.listFeatures(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to ListFeatures method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (RetrievalException e) {
      log.error("Unable to fetch entities requested in ListFeatures method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ListFeatures method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /** Retrieve a list of entities */
  @Override
  public void listEntities(
      ListEntitiesRequest request, StreamObserver<ListEntitiesResponse> responseObserver) {
    try {
      ListEntitiesResponse response = specService.listEntities(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Illegal arguments provided to ListEntities method: ", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (RetrievalException e) {
      log.error("Unable to fetch entities requested in ListEntities method: ", e);
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ListEntities method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  /* Registers an entity to Feast Core */
  @Override
  public void applyEntity(
      ApplyEntityRequest request, StreamObserver<ApplyEntityResponse> responseObserver) {

    String projectId;

    try {
      EntitySpec spec = request.getSpec();
      projectId = request.getProject();
      ApplyEntityResponse response = specService.applyEntity(spec, projectId);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (org.hibernate.exception.ConstraintViolationException e) {
      log.error(
          "Unable to persist this entity due to a constraint violation. Please ensure that"
              + " field names are unique within the project namespace: ",
          e);
      responseObserver.onError(
          Status.ALREADY_EXISTS.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in ApplyEntity method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void createProject(
      CreateProjectRequest request, StreamObserver<CreateProjectResponse> responseObserver) {
    try {
      projectService.createProject(request.getName());
      responseObserver.onNext(CreateProjectResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in the createProject method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void archiveProject(
      ArchiveProjectRequest request, StreamObserver<ArchiveProjectResponse> responseObserver) {
    String projectId;
    try {
      projectId = request.getName();
      projectService.archiveProject(projectId);
      responseObserver.onNext(ArchiveProjectResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("Recieved an invalid request on calling archiveProject method:", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (UnsupportedOperationException e) {
      log.error("Attempted to archive an unsupported project:", e);
      responseObserver.onError(
          Status.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in the createProject method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listProjects(
      ListProjectsRequest request, StreamObserver<ListProjectsResponse> responseObserver) {
    try {
      List<Project> projects = projectService.listProjects();
      responseObserver.onNext(
          ListProjectsResponse.newBuilder()
              .addAllProjects(projects.stream().map(Project::getName).collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in the listProjects method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void applyFeatureTable(
      ApplyFeatureTableRequest request,
      StreamObserver<ApplyFeatureTableResponse> responseObserver) {

    String projectName = SpecService.resolveProjectName(request.getProject());
    String tableName = request.getTableSpec().getName();
    try {
      ApplyFeatureTableResponse response = specService.applyFeatureTable(request);
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
    } catch (IllegalArgumentException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Invalid apply Feature Table Request: (name: %s, project: %s)",
              tableName, projectName));
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (UnsupportedOperationException e) {
      log.error(
          String.format(
              "ApplyFeatureTable: Unsupported apply Feature Table Request: (name: %s, project: %s)",
              tableName, projectName));
      responseObserver.onError(
          Status.UNIMPLEMENTED.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("ApplyFeatureTable Exception has occurred:", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listFeatureTables(
      ListFeatureTablesRequest request,
      StreamObserver<ListFeatureTablesResponse> responseObserver) {

    try {
      ListFeatureTablesResponse response = specService.listFeatureTables(request.getFilter());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (IllegalArgumentException e) {
      log.error("ListFeatureTable: Invalid list Feature Table Request");
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    } catch (Exception e) {
      log.error("ListFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getFeatureTable(
      GetFeatureTableRequest request, StreamObserver<GetFeatureTableResponse> responseObserver) {
    try {
      GetFeatureTableResponse response = specService.getFeatureTable(request);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(
          String.format(
              "GetFeatureTable: No such Feature Table: (project: %s, name: %s)",
              request.getProject(), request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("GetFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void deleteFeatureTable(
      DeleteFeatureTableRequest request,
      StreamObserver<DeleteFeatureTableResponse> responseObserver) {
    try {
      specService.deleteFeatureTable(request);

      responseObserver.onNext(DeleteFeatureTableResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(
          String.format(
              "DeleteFeatureTable: No such Feature Table: (project: %s, name: %s)",
              request.getProject(), request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("DeleteFeatureTable: Exception has occurred: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void listOnlineStores(
      ListOnlineStoresRequest request, StreamObserver<ListOnlineStoresResponse> responseObserver) {
    try {
      ListOnlineStoresResponse response = specService.listOnlineStores();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in ListOnlineStores method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void getOnlineStore(
      GetOnlineStoreRequest request, StreamObserver<GetOnlineStoreResponse> responseObserver) {
    try {
      GetOnlineStoreResponse response = specService.getOnlineStore(request.getName());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(String.format("GetOnlineStore: No such online store: %s", request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in getOnlineStore method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void registerOnlineStore(
      RegisterOnlineStoreRequest request,
      StreamObserver<RegisterOnlineStoreResponse> responseObserver) {
    try {
      RegisterOnlineStoreResponse response = specService.registerOnlineStore(request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.error("Exception has occurred in registerOnlineStore method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  @Override
  public void archiveOnlineStore(
      ArchiveOnlineStoreRequest request,
      StreamObserver<ArchiveOnlineStoreResponse> responseObserver) {
    try {
      ArchiveOnlineStoreResponse response = specService.archiveOnlineStore(request.getName());
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (NoSuchElementException e) {
      log.error(String.format("ArchiveOnlineStore: No such online store: %s", request.getName()));
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    } catch (Exception e) {
      log.error("Exception has occurred in archiveOnlineStore method: ", e);
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
    }
  }

  public String getVersion() {
    return version;
  }
}
