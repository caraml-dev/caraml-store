package dev.caraml.store.mlp;

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
      return Map.of("team", mlpProject.team(), "stream", mlpProject.stream());
    } catch (RuntimeException e) {
      log.warn(
          String.format(
              "Exception encountered while retrieving project context for %s: %s",
              projectName, e.getMessage()));
      return Map.of("team", config.getFallbackTeam(), "stream", config.getFallbackStream());
    }
  }
}
