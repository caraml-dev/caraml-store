package dev.caraml.store.feature;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

/** JPA repository for querying FeatureTables stored. */
public interface FeatureTableRepository extends JpaRepository<FeatureTable, Long> {
  // Find single FeatureTable by project and name
  Optional<FeatureTable> findFeatureTableByNameAndProject_Name(String name, String projectName);

  // Find FeatureTables by project
  @EntityGraph(
      type = EntityGraph.EntityGraphType.FETCH,
      attributePaths = {"features", "entities", "streamSource", "batchSource"})
  List<FeatureTable> findAllByProject_Name(String projectName);
}
