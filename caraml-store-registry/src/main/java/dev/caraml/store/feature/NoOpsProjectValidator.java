package dev.caraml.store.feature;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

// By default, we do not perform any validation on the project
@Component
@ConditionalOnMissingBean(name = "ProjectValidator")
public class NoOpsProjectValidator implements ProjectValidator {
  @Override
  public void validateProject(String project) throws IllegalArgumentException {}
}
