package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import dev.caraml.store.sparkjob.hash.HashUtils;

public record IngestionJobTemplate(String store, SparkApplicationSpec sparkApplicationSpec) {

  public SparkApplicationSpec render(String project, String featureTable) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String templateString = mapper.writeValueAsString(this.sparkApplicationSpec);
      String renderedTemplate =
          templateString
              .replaceAll("\\$\\{project}", project)
              .replaceAll("\\$\\{featureTable}", featureTable)
              .replaceAll("\\$\\{hash}", HashUtils.projectFeatureTableHash(project, featureTable));
      return mapper.readValue(renderedTemplate, SparkApplicationSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
