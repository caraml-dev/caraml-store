package dev.caraml.store.service;

import dev.caraml.store.dao.ProjectRepository;
import dev.caraml.store.model.Project;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class ProjectService {

  private final ProjectRepository projectRepository;

  @Autowired
  public ProjectService(ProjectRepository projectRepository) {
    this.projectRepository = projectRepository;
  }

  /**
   * Creates a project
   *
   * @param name Name of project to be created
   */
  @Transactional
  public void createProject(String name) {
    if (projectRepository.existsById(name)) {
      throw new IllegalArgumentException(String.format("Project already exists: %s", name));
    }
    Project project = new Project(name);
    projectRepository.saveAndFlush(project);
  }

  /**
   * Archives a project
   *
   * @param name Name of the project to be archived
   */
  @Transactional
  public void archiveProject(String name) {
    Optional<Project> project = projectRepository.findById(name);
    if (project.isEmpty()) {
      throw new IllegalArgumentException(String.format("Could not find project: \"%s\"", name));
    }
    if (name.equals(Project.DEFAULT_NAME)) {
      throw new UnsupportedOperationException("Archiving the default project is not allowed.");
    }
    Project p = project.get();
    p.setArchived(true);
    projectRepository.saveAndFlush(p);
  }

  /**
   * List all active projects
   *
   * @return List of active projects
   */
  @Transactional
  public List<Project> listProjects() {
    return projectRepository.findAllByArchivedIsFalse();
  }
}
