package dev.caraml.store.config;

import dev.caraml.store.kubernetes.sparkapplication.SparkApplicationSpec;

public record SparkJobConfig(
    String store, String namespace, SparkApplicationSpec sparkApplicationSpec) {}
