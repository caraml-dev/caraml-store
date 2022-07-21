package dev.caraml.store.config;

import java.util.List;

public record JobServiceConfig(List<SparkJobConfig> streamingJobs) {}
