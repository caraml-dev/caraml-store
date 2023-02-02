package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;

public record HistoricalRetrievalJobTemplate(SparkApplicationSpec sparkApplicationSpec) {

  public SparkApplicationSpec render(String project) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String templateString = mapper.writeValueAsString(this.sparkApplicationSpec);
      String renderedTemplate = templateString.replaceAll("\\$\\{project}", project);
      return mapper.readValue(renderedTemplate, SparkApplicationSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
