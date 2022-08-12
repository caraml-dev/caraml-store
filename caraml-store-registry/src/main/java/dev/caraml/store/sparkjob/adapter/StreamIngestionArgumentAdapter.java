package dev.caraml.store.sparkjob.adapter;

import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamIngestionArgumentAdapter implements SparkApplicationArgumentAdapter {

  private FeatureTableArgumentAdapter featureTableArgumentAdapter;
  private IngestionSourceArgumentAdapter ingestionSourceArgumentAdapter;

  public StreamIngestionArgumentAdapter(
      String project, FeatureTableSpec spec, Map<String, String> entityNameToType) {
    featureTableArgumentAdapter = new FeatureTableArgumentAdapter(project, spec, entityNameToType);
    ingestionSourceArgumentAdapter = new IngestionSourceArgumentAdapter(spec.getStreamSource());
  }

  @Override
  public List<String> getArguments() {
    return Stream.concat(
            featureTableArgumentAdapter.getArguments().stream(),
            ingestionSourceArgumentAdapter.getArguments().stream())
        .collect(Collectors.toList());
  }
}
