package dev.caraml.store.mlp;

import java.util.List;

public interface ProjectProvider {

  List<Project> listProjects();

  Project getProject(String name);
}
