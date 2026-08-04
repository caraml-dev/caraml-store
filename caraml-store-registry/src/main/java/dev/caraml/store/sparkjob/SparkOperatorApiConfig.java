package dev.caraml.store.sparkjob;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the {@link SparkOperatorApiRegistry}, instantiating one {@link SparkOperatorApi} per
 * configured cluster. Falls back to a single synthesised "default" cluster (from the legacy {@code
 * caraml.kubernetes.inCluster} flag) when no {@code clusters} map is configured.
 */
@Configuration
public class SparkOperatorApiConfig {

  static final String LEGACY_DEFAULT_CLUSTER = "default";

  @Bean
  public SparkOperatorApiRegistry sparkOperatorApiRegistry(
      ClusterConfig clusterConfig, JobServiceConfig jobServiceConfig) throws IOException {
    String fallbackNamespace = jobServiceConfig.getNamespace();
    Map<String, SparkOperatorApi> apiByCluster = new HashMap<>();
    Map<String, String> namespaceByCluster = new HashMap<>();

    Map<String, ClusterConfig.Cluster> clusters = clusterConfig.getClusters();

    if (clusters == null || clusters.isEmpty()) {
      // Backward-compatible fallback: synthesise a single default cluster from the
      // legacy inCluster flag and the jobService namespace.
      ClusterConfig.Cluster legacy = new ClusterConfig.Cluster();
      legacy.setInCluster(Boolean.TRUE.equals(clusterConfig.getInCluster()));
      legacy.setNamespace(fallbackNamespace);
      apiByCluster.put(LEGACY_DEFAULT_CLUSTER, new SparkOperatorApiImpl(legacy));
      namespaceByCluster.put(LEGACY_DEFAULT_CLUSTER, fallbackNamespace);
      return new SparkOperatorApiRegistry(apiByCluster, namespaceByCluster, LEGACY_DEFAULT_CLUSTER);
    }

    for (Map.Entry<String, ClusterConfig.Cluster> entry : clusters.entrySet()) {
      ClusterConfig.Cluster cluster = entry.getValue();
      String namespace =
          (cluster.getNamespace() != null && !cluster.getNamespace().isEmpty())
              ? cluster.getNamespace()
              : fallbackNamespace;
      apiByCluster.put(entry.getKey(), new SparkOperatorApiImpl(cluster));
      namespaceByCluster.put(entry.getKey(), namespace);
    }

    String defaultCluster = clusterConfig.getDefaultCluster();
    if (defaultCluster == null || defaultCluster.isEmpty()) {
      if (clusters.size() == 1) {
        defaultCluster = clusters.keySet().iterator().next();
      } else {
        throw new IllegalArgumentException(
            "caraml.kubernetes.defaultCluster must be set when multiple clusters are configured");
      }
    }
    if (!apiByCluster.containsKey(defaultCluster)) {
      throw new IllegalArgumentException(
          String.format(
              "caraml.kubernetes.defaultCluster '%s' is not a configured cluster", defaultCluster));
    }

    return new SparkOperatorApiRegistry(apiByCluster, namespaceByCluster, defaultCluster);
  }
}
