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
      Project mlpProject = projectProvider.getProject(projectName);
      return Map.of("team", mlpProject.team(), "stream", mlpProject.stream());
    } catch (Exception e) {
      log.warn(
          String.format("unable to get project context for %s: %s", projectName, e.getMessage()));
      return Map.of("team", config.getFallbackTeam(), "stream", config.getFallbackStream());
    }
  }
}
