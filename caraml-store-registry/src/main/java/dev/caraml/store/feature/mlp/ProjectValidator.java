package dev.caraml.store.feature.mlp;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "caraml.mlp", name = "enabled", havingValue = "true")
public class ProjectValidator implements dev.caraml.store.feature.ProjectValidator {

  ProjectProvider projectProvider;

  public ProjectValidator(ProjectProvider projectProvider) {
    this.projectProvider = projectProvider;
  }

  @Override
  public void validateProject(String projectName) throws IllegalArgumentException {

    if (projectProvider.listProjects().stream()
        .noneMatch(project -> project.name().equals(projectName))) {
      throw new IllegalArgumentException(
          String.format(
              "%s does not exist on MLP project. Please create the project via the console.",
              projectName));
    }
  }
}
