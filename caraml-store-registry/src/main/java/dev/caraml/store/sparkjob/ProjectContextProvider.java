package dev.caraml.store.sparkjob;

import java.util.Map;

public interface ProjectContextProvider {

  /**
   * Provide additional context for a project to be passed as input to the spark application
   * template
   *
   * @param projectName Feast project name
   * @return Context information in the form of map
   */
  Map<String, String> getContext(String projectName);
}
