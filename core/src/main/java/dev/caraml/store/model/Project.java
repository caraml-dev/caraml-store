package dev.caraml.store.model;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@javax.persistence.Entity
@Table(name = "projects")
public class Project {
  public static final String DEFAULT_NAME = "default";

  // Name of the project
  @Id
  @Column(name = "name", nullable = false, unique = true)
  private String name;

  // Flag to set whether the project has been archived
  @Column(name = "archived", nullable = false)
  private boolean archived;

  @OneToMany(
      cascade = CascadeType.ALL,
      fetch = FetchType.LAZY,
      orphanRemoval = true,
      mappedBy = "project")
  private Set<Entity> entities;

  @OneToMany(
      cascade = CascadeType.ALL,
      fetch = FetchType.LAZY,
      orphanRemoval = true,
      mappedBy = "project")
  private Set<FeatureTable> featureTables;

  public Project() {
    super();
  }

  public Project(String name) {
    this.name = name;
    this.entities = new HashSet<>();
    this.featureTables = new HashSet<>();
  }

  public void addEntity(Entity entity) {
    entity.setProject(this);
    entities.add(entity);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Project field = (Project) o;
    return name.equals(field.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }
}
