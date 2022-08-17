package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class HistoricalRetrievalArgumentAdapter implements SparkApplicationArgumentAdapter {

  private final String project;
  private final List<FeatureTableSpec> specs;
  private final Map<String, String> entityNameToType;
  private final DataSource entitySource;
  private final String outputFormat;
  private final String outputUri;

  record Destination(String format, String path) {}

  @Override
  public List<String> getArguments() {
    FeatureTableConverter featureTableConverter = new FeatureTableConverter();
    DataSourceConverter dataSourceConverter = new DataSourceConverter();
    try {
      Stream<String> featureTableArguments =
          Stream.of(
              "--feature-tables", featureTableConverter.convert(project, specs, entityNameToType));
      Stream<String> featureTableSourceArguments =
          Stream.of(
              "--feature-tables-sources",
              dataSourceConverter.convert(
                  specs.stream().map(FeatureTableSpec::getBatchSource).toList()));
      Stream<String> entitySourceArguments =
          Stream.of("--entity-source", dataSourceConverter.convert(entitySource));
      Stream<String> destinationArguments =
          Stream.of(
              "--destination",
              new ObjectMapper().writeValueAsString(new Destination(outputFormat, outputUri)));
      return Stream.of(
              featureTableArguments,
              featureTableSourceArguments,
              entitySourceArguments,
              destinationArguments)
          .flatMap(Function.identity())
          .toList();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
