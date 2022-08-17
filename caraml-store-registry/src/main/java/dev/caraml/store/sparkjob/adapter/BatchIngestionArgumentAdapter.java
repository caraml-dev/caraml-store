package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BatchIngestionArgumentAdapter implements SparkApplicationArgumentAdapter {

  private final String project;
  private final FeatureTableSpec spec;
  private final Map<String, String> entityNameToType;
  private final Timestamp startTime;
  private final Timestamp endTime;

  @Override
  public List<String> getArguments() {

    try {
      Stream<String> featureTableArguments =
          Stream.of(
              "--feature-table",
              new FeatureTableConverter().convert(project, spec, entityNameToType));
      Stream<String> sourceArguments =
          Stream.of("--source", new DataSourceConverter().convert(spec.getBatchSource()));
      Stream<String> timestampArguments =
          Stream.of(
              "--start", Timestamps.toString(startTime), "--end", Timestamps.toString(endTime));
      return Stream.of(featureTableArguments, sourceArguments, timestampArguments)
          .flatMap(Function.identity())
          .toList();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
