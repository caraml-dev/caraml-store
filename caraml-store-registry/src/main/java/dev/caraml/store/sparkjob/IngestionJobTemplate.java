package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;

/**
 * Ingestion job template. {@code defaultCluster} is the job-type-level default cluster for this
 * store; when null/empty the root default cluster (caraml.kubernetes.defaultCluster) is used.
 */
public record IngestionJobTemplate(
    String store, String defaultCluster, SparkApplicationSpec sparkApplicationSpec) {}
