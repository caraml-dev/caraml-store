package dev.caraml.store.mlp;

import dev.caraml.store.feature.ValidationResult;
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
  public ValidationResult validate(String projectName) {
    if (projectProvider.getProject(projectName).isPresent()) {
      return new ValidationResult(true, "");
    } else {
      return new ValidationResult(false, String.format("%s is not a valid MLP project"));
    }
  }
}
