package dev.caraml.store.model;

import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import dev.caraml.store.util.TypeConversion;
import java.util.Map;
import java.util.Objects;
import javax.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

/** Defines a single Feature defined in a {@link FeatureTable} */
@Getter
@javax.persistence.Entity
@Setter(AccessLevel.PRIVATE)
@Table(
    name = "features",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "feature_table_id"}))
public class Feature {
  @Id @GeneratedValue private long id;

  // Name of the Feature
  private String name;

  // Feature Table where this Feature is defined in.
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "feature_table_id", nullable = false)
  private FeatureTable featureTable;

  // Value type of the feature. String representation of ValueType.
  @Enumerated(EnumType.STRING)
  @Column(name = "type")
  private ValueType.Enum type;

  // User defined metadata labels for this feature encoded a JSON string.
  @Column(name = "labels", columnDefinition = "text")
  private String labelsJSON;

  public Feature() {}

  public Feature(FeatureTable table, String name, ValueType.Enum type, String labelsJSON) {
    this.featureTable = table;
    this.name = name;
    this.type = type;
    this.labelsJSON = labelsJSON;
  }

  /**
   * Construct Feature from Protobuf spec representation.
   *
   * @param table the FeatureTable to associate the constructed feature with.
   * @param spec the Protobuf spec to contruct the Feature from.
   * @return constructed Feature from the given Protobuf spec.
   */
  public static Feature fromProto(FeatureTable table, FeatureSpec spec) {
    String labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
    return new Feature(table, spec.getName(), spec.getValueType(), labelsJSON);
  }

  /** Convert this Feature to its Protobuf representation. */
  public FeatureSpec toProto() {
    Map<String, String> labels = TypeConversion.convertJsonStringToMap(getLabelsJSON());
    return FeatureSpec.newBuilder()
        .setName(getName())
        .setValueType(getType())
        .putAllLabels(labels)
        .build();
  }

  /**
   * Update the Feature from the given Protobuf representation.
   *
   * @param spec the Protobuf spec to update the Feature from.
   * @throws IllegalArgumentException if the update will make prohibited changes.
   */
  public void updateFromProto(FeatureSpec spec) {
    // Check for prohibited changes made in spec
    if (!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Updating the name of a registered Feature is not allowed: %s to %s",
              getName(), spec.getName()));
    }
    // Update feature type
    this.setType(spec.getValueType());

    // Update Feature based on spec
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
  }

  /**
   * Return a boolean to indicate if Feature contains all specified labels.
   *
   * @param labelsFilter contain labels that should be attached to Feature
   * @return boolean True if Feature contains all labels in the labelsFilter
   */
  public boolean hasAllLabels(Map<String, String> labelsFilter) {
    Map<String, String> featureLabelsMap = TypeConversion.convertJsonStringToMap(getLabelsJSON());
    for (String key : labelsFilter.keySet()) {
      if (!featureLabelsMap.containsKey(key)
          || !featureLabelsMap.get(key).equals(labelsFilter.get(key))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getType(), getLabelsJSON());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Feature feature = (Feature) o;
    return getName().equals(feature.getName())
        && getType().equals(feature.getType())
        && getLabelsJSON().equals(feature.getLabelsJSON());
  }
}
