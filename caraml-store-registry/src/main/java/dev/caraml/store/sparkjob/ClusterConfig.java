package dev.caraml.store.sparkjob;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.kubernetes")
@Getter
@Setter
public class ClusterConfig {

  // Legacy single-cluster flag. Retained for backward compatibility: when no
  // `clusters` map is configured, a single default cluster is synthesised from
  // this flag (and the jobService namespace).
  private Boolean inCluster;

  // Name of the cluster to use when a retrieval request does not specify one.
  // Optional when exactly one cluster is configured.
  private String defaultCluster;

  // Named compute clusters that retrieval jobs can be submitted to. When empty,
  // a single "default" cluster is synthesised from the legacy `inCluster` flag.
  private Map<String, Cluster> clusters;

  @Getter
  @Setter
  public static class Cluster {
    // Whether to use in-cluster Kubernetes configuration for this cluster.
    private Boolean inCluster = false;

    // kubeconfig context name to use for this cluster. When null/empty, the
    // default client resolution is used (KUBECONFIG current-context).
    private String context;

    // Namespace that Spark applications are created in for this cluster. When
    // null/empty, falls back to caraml.jobService.namespace.
    private String namespace;
  }
}
