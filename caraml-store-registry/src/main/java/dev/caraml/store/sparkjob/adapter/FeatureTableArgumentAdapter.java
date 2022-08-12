package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class FeatureTableArgumentAdapter implements SparkApplicationArgumentAdapter {

  private String project;
  private FeatureTableSpec spec;
  private Map<String, String> entityNameToType;

  record Field(String name, String type) {}

  record FeatureTableArgument(
      String project,
      String name,
      Map<String, String> labels,
      Integer maxAge,
      List<Field> entities,
      List<Field> features) {}

  @Override
  public List<String> getArguments() {
    FeatureTableArgument arg =
        new FeatureTableArgument(
            project,
            spec.getName(),
            spec.getLabelsMap(),
            Math.toIntExact(spec.getMaxAge().getSeconds()),
            spec.getEntitiesList().stream()
                .map(e -> new Field(e, entityNameToType.get(e)))
                .collect(Collectors.toList()),
            spec.getFeaturesList().stream()
                .map(f -> new Field(f.getName(), f.getValueType().toString()))
                .collect(Collectors.toList()));
    try {
      return List.of("--feature-table", new ObjectMapper().writeValueAsString(arg));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
