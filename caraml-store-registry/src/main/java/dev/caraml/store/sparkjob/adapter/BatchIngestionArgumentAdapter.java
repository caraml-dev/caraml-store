package dev.caraml.store.sparkjob.adapter;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class BatchIngestionArgumentAdapter implements SparkApplicationArgumentAdapter {

  private FeatureTableArgumentAdapter featureTableArgumentAdapter;
  private IngestionSourceArgumentAdapter ingestionSourceArgumentAdapter;
  private Timestamp startTime;
  private Timestamp endTime;

  public BatchIngestionArgumentAdapter(
      String project,
      FeatureTableSpec spec,
      Map<String, String> entityNameToType,
      Timestamp startTime,
      Timestamp endTime) {
    featureTableArgumentAdapter = new FeatureTableArgumentAdapter(project, spec, entityNameToType);
    ingestionSourceArgumentAdapter = new IngestionSourceArgumentAdapter(spec.getBatchSource());
    this.startTime = startTime;
    this.endTime = endTime;
  }

  @Override
  public List<String> getArguments() {
    Stream<String> timestampArguments =
        Stream.of("--start", Timestamps.toString(startTime), "--end", Timestamps.toString(endTime));

    return Stream.of(
            featureTableArgumentAdapter.getArguments().stream(),
            ingestionSourceArgumentAdapter.getArguments().stream(),
            timestampArguments)
        .flatMap(Function.identity())
        .toList();
  }
}
