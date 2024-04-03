package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;

import java.util.Map;

public class JobTemplateRenderer {

  public SparkApplicationSpec render(
      SparkApplicationSpec templateSpec, Map<String, String> context) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      String renderedTemplate = mapper.writeValueAsString(templateSpec);
      for (String key : context.keySet()) {
        String value = context.get(key);
        renderedTemplate = renderedTemplate.replaceAll(String.format("\\$\\{%s}", key), value);
      }
      // replace all unsubstituted label values with empty string
      renderedTemplate = renderedTemplate.replaceAll("\\$\\{.+\\}$", "");
      return mapper.readValue(renderedTemplate, SparkApplicationSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
