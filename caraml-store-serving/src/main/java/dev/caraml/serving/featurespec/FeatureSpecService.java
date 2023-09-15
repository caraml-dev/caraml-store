package dev.caraml.serving.featurespec;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import dev.caraml.store.protobuf.core.CoreServiceGrpc;
import dev.caraml.store.protobuf.core.CoreServiceGrpc.CoreServiceBlockingStub;
import dev.caraml.store.protobuf.core.CoreServiceProto.GetFeatureTableRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesRequest;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListProjectsRequest;
import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTable;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/** In-memory cache of specs hosted in CaraML Store Registry. */
@Slf4j
@Service
public class FeatureSpecService {

  private final CoreServiceBlockingStub registryClient;

  private HashMap<String, List<String>> projectFeatureTableCache;

  private final LoadingCache<ImmutablePair<String, String>, FeatureTableSpec> featureTableCache;

  private final LoadingCache<ImmutablePair<String, FeatureReference>, FeatureSpec> featureCache;

  @Autowired
  public FeatureSpecService(FeatureSpecServiceConfig config) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext().build();
    registryClient = CoreServiceGrpc.newBlockingStub(channel);

    projectFeatureTableCache = new HashMap<>();

    CacheLoader<ImmutablePair<String, String>, FeatureTableSpec> featureTableCacheLoader =
        CacheLoader.from(k -> retrieveSingleFeatureTable(k.getLeft(), k.getRight()));
    featureTableCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getCache().expiry(), TimeUnit.HOURS)
            .build(featureTableCacheLoader);

    CacheLoader<ImmutablePair<String, FeatureReference>, FeatureSpec> featureCacheLoader =
        CacheLoader.from(k -> retrieveSingleFeature(k.getLeft(), k.getRight()));
    featureCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getCache().expiry(), TimeUnit.HOURS)
            .build(featureCacheLoader);
  }

  /**
   * Reload the store configuration from the given config path, then retrieve the necessary specs
   * from core to preload the cache.
   */
  public void populateCache() {
    ImmutableTriple<
            HashMap<ImmutablePair<String, String>, FeatureTableSpec>,
            HashMap<String, List<String>>,
            HashMap<ImmutablePair<String, FeatureReference>, FeatureSpec>>
        specs = getFeatureTableMap();

    projectFeatureTableCache = specs.getMiddle();
    featureTableCache.putAll(specs.getLeft());
    featureCache.putAll(specs.getRight());
  }

  @Scheduled(
      initialDelayString = "${caraml.registry.cache.initialDelay}",
      fixedRateString = "${caraml.registry.cache.refreshInterval}")
  public void scheduledPopulateCache() {
    try {
      populateCache();
    } catch (Exception e) {
      log.warn("Error updating store configuration and specs: {}", e.getMessage());
    }
  }

  /**
   * Provides a map for easy retrieval of FeatureTable spec using FeatureTable reference
   *
   * @return Map in the format of <project/featuretable_name: FeatureTableSpec>
   */
  private ImmutableTriple<
          HashMap<ImmutablePair<String, String>, FeatureTableSpec>,
          HashMap<String, List<String>>,
          HashMap<ImmutablePair<String, FeatureReference>, FeatureSpec>>
      getFeatureTableMap() {
    HashMap<ImmutablePair<String, String>, FeatureTableSpec> featureTables = new HashMap<>();
    HashMap<ImmutablePair<String, FeatureReference>, FeatureSpec> features = new HashMap<>();
    HashMap<String, List<String>> projectFeatureTables = new HashMap<>();

    List<String> projects =
        registryClient.listProjects(ListProjectsRequest.newBuilder().build()).getProjectsList();

    for (String project : projects) {
      ListFeatureTablesResponse featureTablesResponse =
          registryClient.listFeatureTables(
              ListFeatureTablesRequest.newBuilder()
                  .setFilter(ListFeatureTablesRequest.Filter.newBuilder().setProject(project))
                  .build());
      List<String> featureTableNames =
          featureTablesResponse.getTablesList().stream()
              .map(featureTable -> featureTable.getSpec().getName())
              .collect(Collectors.toList());
      projectFeatureTables.put(project, featureTableNames);
      Map<FeatureReference, FeatureSpec> featureRefSpecMap = new HashMap<>();
      for (FeatureTable featureTable : featureTablesResponse.getTablesList()) {
        FeatureTableSpec spec = featureTable.getSpec();
        featureTables.put(ImmutablePair.of(project, spec.getName()), spec);

        String featureTableName = spec.getName();
        List<FeatureSpec> featureSpecs = spec.getFeaturesList();
        for (FeatureSpec featureSpec : featureSpecs) {
          FeatureReference featureReference =
              FeatureReference.newBuilder()
                  .setFeatureTable(featureTableName)
                  .setName(featureSpec.getName())
                  .build();
          features.put(ImmutablePair.of(project, featureReference), featureSpec);
        }
      }
    }
    return ImmutableTriple.of(featureTables, projectFeatureTables, features);
  }

  private FeatureTableSpec retrieveSingleFeatureTable(String projectName, String tableName) {
    FeatureTable table =
        registryClient
            .getFeatureTable(
                GetFeatureTableRequest.newBuilder()
                    .setProject(projectName)
                    .setName(tableName)
                    .build())
            .getTable();
    return table.getSpec();
  }

  private FeatureSpec retrieveSingleFeature(String projectName, FeatureReference featureReference) {
    FeatureTableSpec featureTableSpec =
        getFeatureTableSpec(projectName, featureReference); // don't stress core too much
    if (featureTableSpec == null) {
      return null;
    }
    return featureTableSpec.getFeaturesList().stream()
        .filter(f -> f.getName().equals(featureReference.getName()))
        .findFirst()
        .orElse(null);
  }

  public FeatureTableSpec getFeatureTableSpec(
      String projectName, FeatureReference featureReference) {
    FeatureTableSpec featureTableSpec;
    try {
      featureTableSpec =
          featureTableCache.get(ImmutablePair.of(projectName, featureReference.getFeatureTable()));
      return featureTableSpec;
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof StatusRuntimeException grpcError) {
        if (grpcError.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
          throw new FeatureRefNotFoundException(projectName, featureReference);
        } else {
          throw e;
        }
      }
      throw e;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public FeatureSpec getFeatureSpec(String projectName, FeatureReference featureReference) {
    FeatureSpec featureSpec;
    try {
      featureSpec = featureCache.get(ImmutablePair.of(projectName, featureReference));
    } catch (UncheckedExecutionException e) {
      if (e.getCause() instanceof StatusRuntimeException grpcError) {
        if (grpcError.getStatus().getCode().equals(Status.Code.NOT_FOUND)) {
          throw new FeatureRefNotFoundException(projectName, featureReference);
        } else {
          throw e;
        }
      }
      throw e;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

    return featureSpec;
  }

  public List<String> getProjects() {
    List<String> projects = new ArrayList<>(projectFeatureTableCache.keySet());
    return projects;
  }

  public List<String> getFeatureTables(String project) {
    return projectFeatureTableCache.get(project);
  }
}
