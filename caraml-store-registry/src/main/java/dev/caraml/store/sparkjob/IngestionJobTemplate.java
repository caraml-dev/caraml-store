package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;

public record IngestionJobTemplate(String store, SparkApplicationSpec sparkApplicationSpec) {}
