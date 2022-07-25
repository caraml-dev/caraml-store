package dev.caraml.store.feature;

import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.core.EntityProto;
import dev.caraml.store.protobuf.core.EntityProto.EntityMeta;
import dev.caraml.store.protobuf.core.EntityProto.EntitySpec;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@javax.persistence.Entity
@Table(
    name = "entities",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
public class Entity extends AbstractTimestampEntity {
  @Id @GeneratedValue private long id;

  // Name of the Entity
  @Column(name = "name", nullable = false)
  private String name;

  // Project that this Entity belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Description of entity
  @Column(name = "description", columnDefinition = "text")
  private String description;

  // Columns of entities
  /** Data type of each entity column: String representation of {@link ValueType} * */
  private String type;

  // User defined metadata
  @Column(name = "labels", columnDefinition = "text")
  private String labels;

  public Entity() {
    super();
  }

  /**
   * Entity object supports Entity registration in FeatureTable.
   *
   * <p>This data model supports Scalar Entity and would allow ease of discovery of entities and
   * reasoning when used in association with FeatureTable.
   */
  public Entity(String name, String description, ValueType.Enum type, Map<String, String> labels) {
    this.name = name;
    this.description = description;
    this.type = type.toString();
    this.labels = TypeConversion.convertMapToJsonString(labels);
  }

  public static Entity fromProto(EntityProto.Entity entityProto) {
    EntitySpec spec = entityProto.getSpec();

    return new Entity(
        spec.getName(), spec.getDescription(), spec.getValueType(), spec.getLabelsMap());
  }

  public EntityProto.Entity toProto() {
    EntityMeta.Builder meta =
        EntityMeta.newBuilder()
            .setCreatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getCreated().getTime() / 1000L))
            .setLastUpdatedTimestamp(
                Timestamp.newBuilder().setSeconds(super.getLastUpdated().getTime() / 1000L));

    EntitySpec.Builder spec =
        EntitySpec.newBuilder()
            .setName(getName())
            .setDescription(getDescription())
            .setValueType(ValueType.Enum.valueOf(getType()))
            .putAllLabels(TypeConversion.convertJsonStringToMap(labels));

    // Build Entity
    EntityProto.Entity entity = EntityProto.Entity.newBuilder().setMeta(meta).setSpec(spec).build();
    return entity;
  }

  /**
   * Updates the existing entity from a proto.
   *
   * @param entityProto EntityProto with updated spec
   * @param projectName Project namespace of Entity which is to be created/updated
   */
  public void updateFromProto(EntityProto.Entity entityProto, String projectName) {
    EntitySpec spec = entityProto.getSpec();

    // Validate no change to type
    if (!spec.getValueType().equals(ValueType.Enum.valueOf(getType()))) {
      throw new IllegalArgumentException(
          String.format(
              "You are attempting to change the type of this entity in %s project from %s to %s. This isn't allowed. Please create a new entity.",
              projectName, getType(), spec.getValueType()));
    }

    // 2. Update description, labels
    this.setDescription(spec.getDescription());
    this.setLabels(TypeConversion.convertMapToJsonString(spec.getLabelsMap()));
  }

  /**
   * Determine whether an entity has all the specified labels.
   *
   * @param labelsFilter labels contain key-value mapping for labels attached to the Entity
   * @return boolean True if Entity contains all labels in the labelsFilter
   */
  public boolean hasAllLabels(Map<String, String> labelsFilter) {
    Map<String, String> LabelsMap = this.getLabelsMap();
    for (String key : labelsFilter.keySet()) {
      if (!LabelsMap.containsKey(key) || !LabelsMap.get(key).equals(labelsFilter.get(key))) {
        return false;
      }
    }
    return true;
  }

  public Map<String, String> getLabelsMap() {
    return TypeConversion.convertJsonStringToMap(this.getLabels());
  }
}
