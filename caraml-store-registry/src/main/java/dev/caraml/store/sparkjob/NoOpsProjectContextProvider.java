package dev.caraml.store.sparkjob;

import java.util.Collections;
import java.util.Map;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnMissingBean(name = "ProjectContextProvider")
public class NoOpsProjectContextProvider implements ProjectContextProvider {
  @Override
  public Map<String, String> getContext(String projectName) {
    return Collections.emptyMap();
  }
}
