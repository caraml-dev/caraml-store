package dev.caraml.store.dao;

import dev.caraml.store.model.Project;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying Project objects keyed by id. */
public interface ProjectRepository extends JpaRepository<Project, String> {

  List<Project> findAllByArchivedIsFalse();
}
