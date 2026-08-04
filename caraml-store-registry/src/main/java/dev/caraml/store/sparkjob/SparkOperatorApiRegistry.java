package dev.caraml.store.sparkjob;

import java.util.Map;
import java.util.Set;

/**
 * Holds one {@link SparkOperatorApi} per configured compute cluster, keyed by cluster name, along
 * with the namespace to use for each. Retrieval jobs are routed to a caller-selected cluster;
 * status/list/delete operations fan out across all clusters (see {@link JobService}).
 */
public class SparkOperatorApiRegistry {

  private final Map<String, SparkOperatorApi> apiByCluster;
  private final Map<String, String> namespaceByCluster;
  private final String defaultCluster;

  public SparkOperatorApiRegistry(
      Map<String, SparkOperatorApi> apiByCluster,
      Map<String, String> namespaceByCluster,
      String defaultCluster) {
    this.apiByCluster = apiByCluster;
    this.namespaceByCluster = namespaceByCluster;
    this.defaultCluster = defaultCluster;
  }

  /** Resolves a (possibly empty) cluster name to a concrete configured cluster name. */
  public String resolve(String cluster) {
    return (cluster == null || cluster.isEmpty()) ? defaultCluster : cluster;
  }

  /** Returns the API client for the given cluster (empty => default cluster). */
  public SparkOperatorApi get(String cluster) {
    String name = resolve(cluster);
    SparkOperatorApi api = apiByCluster.get(name);
    if (api == null) {
      throw new IllegalArgumentException(String.format("Unknown cluster: %s", cluster));
    }
    return api;
  }

  /** Returns the namespace for the given cluster (empty => default cluster). */
  public String namespace(String cluster) {
    String name = resolve(cluster);
    String namespace = namespaceByCluster.get(name);
    if (namespace == null) {
      throw new IllegalArgumentException(String.format("Unknown cluster: %s", cluster));
    }
    return namespace;
  }

  public String defaultCluster() {
    return defaultCluster;
  }

  public SparkOperatorApi defaultApi() {
    return get(defaultCluster);
  }

  public String defaultNamespace() {
    return namespace(defaultCluster);
  }

  /** Names of all configured clusters, used to fan out status/list/delete operations. */
  public Set<String> clusterNames() {
    return apiByCluster.keySet();
  }
}
