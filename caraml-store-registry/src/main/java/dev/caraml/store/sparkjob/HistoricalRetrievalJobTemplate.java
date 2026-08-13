package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import java.util.Map;

public record HistoricalRetrievalJobTemplate(
    String defaultCluster, SparkApplicationSpec sparkApplicationSpec) {

  public SparkApplicationSpec render(
      String project, ProjectContextProvider projectContextProvider) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String templateString = mapper.writeValueAsString(this.sparkApplicationSpec);
      String renderedTemplate = templateString.replaceAll("\\$\\{project}", project);
      Map<String, String> context = projectContextProvider.getContext(project);
      for (String key : context.keySet()) {
        String value = context.get(key);
        renderedTemplate = renderedTemplate.replaceAll(String.format("\\$\\{%s}", key), value);
      }

      return mapper.readValue(renderedTemplate, SparkApplicationSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
