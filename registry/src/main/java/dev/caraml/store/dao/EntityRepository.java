package dev.caraml.store.dao;

import dev.caraml.store.model.Entity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository supplying Entity objects keyed by id. */
public interface EntityRepository extends JpaRepository<Entity, String> {

  long count();

  // Find all Entities by project
  List<Entity> findAllByProject_Name(String project);

  // Find single Entity by project and name
  Entity findEntityByNameAndProject_Name(String name, String project);
}
