package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class FeatureTableConverter {

  record Field(String name, String type) {}

  record FeatureTable(
      String project,
      String name,
      Map<String, String> labels,
      Integer maxAge,
      List<Field> entities,
      List<Field> features) {}

  private FeatureTable specToArgument(
      String project, FeatureTableSpec spec, Map<String, String> entityNameToType) {
    return new FeatureTable(
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
  }

  public String convert(String project, FeatureTableSpec spec, Map<String, String> entityNameToType)
      throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(specToArgument(project, spec, entityNameToType));
  }

  public String convert(
      String project, List<FeatureTableSpec> specs, Map<String, String> entityNameToType)
      throws JsonProcessingException {
    return new ObjectMapper()
        .writeValueAsString(
            specs.stream().map(spec -> specToArgument(project, spec, entityNameToType)).toList());
  }
}
