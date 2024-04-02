package dev.caraml.store.mlp;

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@ConditionalOnProperty(prefix = "caraml.mlp", name = "enabled", havingValue = "true")
public class ProjectContextProvider implements dev.caraml.store.sparkjob.ProjectContextProvider {

  ProjectProvider projectProvider;
  ContextProviderConfig config;

  public ProjectContextProvider(ProjectProvider projectProvider, ContextProviderConfig config) {
    this.projectProvider = projectProvider;
    this.config = config;
  }

  @Override
  public Map<String, String> getContext(String projectName) {
    try {
      Project mlpProject =
          projectProvider
              .getProject(projectName)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format("%s is not a valid MLP project", projectName)));
      Map<String, String> contextMap = new HashMap<>();
      contextMap.put("team", mlpProject.team());
      contextMap.put("stream", mlpProject.stream());

      for (Label label : mlpProject.labels()) {
        if (check_validity(label.key()) && check_validity(label.value())){
          contextMap.put(label.key(), label.value());
        }
      }

      return contextMap;
    } catch (RuntimeException e) {
      log.warn(
          String.format(
              "Exception encountered while retrieving project context for %s: %s",
              projectName, e.getMessage()));
      return Map.of("team", config.getFallbackTeam(), "stream", config.getFallbackStream());
    }
  }

  public boolean check_validity(String input){
    // Define the regular expression pattern
    String regex = "^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$";

    // Check if the input string matches the regular expression and length is less than 64
    return input.length() < 64 && input.matches(regex);
  }
}
