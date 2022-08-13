package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;

public record IngestionJobProperties(String store, SparkApplicationSpec sparkApplicationSpec) {}
