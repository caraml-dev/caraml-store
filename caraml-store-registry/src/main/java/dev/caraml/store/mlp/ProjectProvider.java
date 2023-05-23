package dev.caraml.store.mlp;

import java.util.Optional;

public interface ProjectProvider {

  // Returns the MLP project requested and throw exception if project is not found
  Optional<Project> getProject(String name);
}
