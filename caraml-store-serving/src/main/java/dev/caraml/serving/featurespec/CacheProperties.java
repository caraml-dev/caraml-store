package dev.caraml.serving.featurespec;

public record CacheProperties(Integer expiry, Integer initialDelay, Integer refreshInterval) {}
