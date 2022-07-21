package dev.caraml.store.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import dev.caraml.store.dao.ProjectRepository;
import dev.caraml.store.model.Project;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ProjectServiceTest {

  @Mock private ProjectRepository projectRepository;
  private ProjectService projectService;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    projectRepository = mock(ProjectRepository.class);
    projectService = new ProjectService(projectRepository);
  }

  @Test
  public void shouldCreateProjectIfItDoesntExist() {
    String projectName = "project1";
    Project project = new Project(projectName);
    when(projectRepository.saveAndFlush(project)).thenReturn(project);
    projectService.createProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(project);
  }

  @Test
  public void shouldNotCreateProjectIfItExist() {
    String projectName = "project1";
    when(projectRepository.existsById(projectName)).thenReturn(true);
    assertThrows(IllegalArgumentException.class, () -> projectService.createProject(projectName));
  }

  @Test
  public void shouldArchiveProjectIfItExists() {
    String projectName = "project1";
    Project project = new Project(projectName);
    when(projectRepository.findById(projectName)).thenReturn(Optional.of(project));
    projectService.archiveProject(projectName);
    verify(projectRepository, times(1)).saveAndFlush(project);
  }

  @Test
  public void shouldNotArchiveDefaultProject() {
    assertThrows(
        IllegalArgumentException.class, () -> projectService.archiveProject(Project.DEFAULT_NAME));
  }

  @Test
  public void shouldNotArchiveProjectIfItIsAlreadyArchived() {
    String projectName = "project1";
    when(projectRepository.findById(projectName)).thenReturn(Optional.empty());
    assertThrows(IllegalArgumentException.class, () -> projectService.archiveProject(projectName));
  }

  @Test
  public void shouldListProjects() {
    String projectName = "project1";
    Project project = new Project(projectName);
    List<Project> expected = List.of(project);
    when(projectRepository.findAllByArchivedIsFalse()).thenReturn(expected);
    List<Project> actual = projectService.listProjects();
    assertEquals(expected, actual);
  }
}
