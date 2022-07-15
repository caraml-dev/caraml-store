package dev.caraml.store.testutils.it;

import dev.caraml.store.protobuf.core.*;
import dev.caraml.store.protobuf.core.CoreServiceProto.ApplyFeatureTableRequest;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleCoreClient {
  private final CoreServiceGrpc.CoreServiceBlockingStub stub;

  public SimpleCoreClient(CoreServiceGrpc.CoreServiceBlockingStub stub) {
    this.stub = stub;
  }

  public CoreServiceProto.ApplyEntityResponse simpleApplyEntity(
      String projectName, EntityProto.EntitySpec spec) {
    return stub.applyEntity(
        CoreServiceProto.ApplyEntityRequest.newBuilder()
            .setProject(projectName)
            .setSpec(spec)
            .build());
  }

  public List<EntityProto.Entity> simpleListEntities(String projectName) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListEntitiesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .build())
                .build())
        .getEntitiesList();
  }

  public List<EntityProto.Entity> simpleListEntities(
      String projectName, Map<String, String> labels) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListEntitiesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .putAllLabels(labels)
                        .build())
                .build())
        .getEntitiesList();
  }

  public List<EntityProto.Entity> simpleListEntities(
      CoreServiceProto.ListEntitiesRequest.Filter filter) {
    return stub.listEntities(
            CoreServiceProto.ListEntitiesRequest.newBuilder().setFilter(filter).build())
        .getEntitiesList();
  }

  public List<FeatureTableProto.FeatureTable> simpleListFeatureTables(
      CoreServiceProto.ListFeatureTablesRequest.Filter filter) {
    return stub.listFeatureTables(
            CoreServiceProto.ListFeatureTablesRequest.newBuilder().setFilter(filter).build())
        .getTablesList();
  }

  public EntityProto.Entity simpleGetEntity(String projectName, String name) {
    return stub.getEntity(
            CoreServiceProto.GetEntityRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getEntity();
  }

  public FeatureTableProto.FeatureTable simpleGetFeatureTable(String projectName, String name) {
    return stub.getFeatureTable(
            CoreServiceProto.GetFeatureTableRequest.newBuilder()
                .setName(name)
                .setProject(projectName)
                .build())
        .getTable();
  }

  public Map<String, FeatureProto.FeatureSpec> simpleListFeatures(
      String projectName, Map<String, String> labels, List<String> entities) {
    return stub.listFeatures(
            CoreServiceProto.ListFeaturesRequest.newBuilder()
                .setFilter(
                    CoreServiceProto.ListFeaturesRequest.Filter.newBuilder()
                        .setProject(projectName)
                        .addAllEntities(entities)
                        .putAllLabels(labels)
                        .build())
                .build())
        .getFeaturesMap();
  }

  public Map<String, FeatureProto.FeatureSpec> simpleListFeatures(
      String projectName, String... entities) {
    return simpleListFeatures(projectName, Collections.emptyMap(), Arrays.asList(entities));
  }

  public void createProject(String name) {
    stub.createProject(CoreServiceProto.CreateProjectRequest.newBuilder().setName(name).build());
  }

  public void archiveProject(String name) {
    stub.archiveProject(CoreServiceProto.ArchiveProjectRequest.newBuilder().setName(name).build());
  }

  public String getFeastCoreVersion() {
    return stub.getFeastCoreVersion(
            CoreServiceProto.GetFeastCoreVersionRequest.getDefaultInstance())
        .getVersion();
  }

  public FeatureTableProto.FeatureTable applyFeatureTable(
      String projectName, FeatureTableSpec spec) {
    return stub.applyFeatureTable(
            ApplyFeatureTableRequest.newBuilder()
                .setProject(projectName)
                .setTableSpec(spec)
                .build())
        .getTable();
  }

  public void deleteFeatureTable(String projectName, String featureTableName) {
    stub.deleteFeatureTable(
        CoreServiceProto.DeleteFeatureTableRequest.newBuilder()
            .setProject(projectName)
            .setName(featureTableName)
            .build());
  }

  public CoreServiceProto.ListOnlineStoresResponse listOnlineStores() {
    return stub.listOnlineStores(CoreServiceProto.ListOnlineStoresRequest.newBuilder().build());
  }

  public CoreServiceProto.GetOnlineStoreResponse getOnlineStore(String name) {
    return stub.getOnlineStore(
        CoreServiceProto.GetOnlineStoreRequest.newBuilder().setName(name).build());
  }

  public CoreServiceProto.RegisterOnlineStoreResponse registerOnlineStore(
      OnlineStoreProto.OnlineStore onlineStore) {
    return stub.registerOnlineStore(
        CoreServiceProto.RegisterOnlineStoreRequest.newBuilder()
            .setOnlineStore(onlineStore)
            .build());
  }

  public CoreServiceProto.ArchiveOnlineStoreResponse archiveOnlineStore(String name) {
    return stub.archiveOnlineStore(
        CoreServiceProto.ArchiveOnlineStoreRequest.newBuilder().setName(name).build());
  }
}
