package dev.caraml.store.feature.mlp;

import java.util.List;

public interface ProjectProvider {

  List<Project> listProjects();

  Project getProject(String name);
}
