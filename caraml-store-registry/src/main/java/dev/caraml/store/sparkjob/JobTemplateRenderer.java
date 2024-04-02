package dev.caraml.store.sparkjob;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import java.util.HashMap;
import java.util.Map;

public class JobTemplateRenderer {

  public SparkApplicationSpec render(
      SparkApplicationSpec templateSpec, Map<String, String> context) {
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> unusedLabels = new HashMap<>();
    try {
      String oldRenderedTemplate;
      String renderedTemplate = mapper.writeValueAsString(templateSpec);
      for (String key : context.keySet()) {
        String value = context.get(key);
        oldRenderedTemplate = renderedTemplate;
        renderedTemplate = renderedTemplate.replaceAll(String.format("\\$\\{%s}", key), value);
        // nothing got replaced
        if (renderedTemplate.equals(oldRenderedTemplate)) unusedLabels.put(key, value);
      }
      SparkApplicationSpec renderedSpecObject =
          mapper.readValue(renderedTemplate, SparkApplicationSpec.class);
      renderedSpecObject.getDriver().getLabels().putAll(unusedLabels);
      renderedSpecObject.getExecutor().getLabels().putAll(unusedLabels);
      return renderedSpecObject;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
