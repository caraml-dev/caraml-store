package dev.caraml.store.feature;

import com.google.common.hash.Hashing;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.core.DataSourceProto;
import dev.caraml.store.protobuf.core.FeatureProto;
import dev.caraml.store.protobuf.core.FeatureTableProto;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.NamedAttributeNode;
import javax.persistence.NamedEntityGraph;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
@javax.persistence.Entity
@Setter(AccessLevel.PRIVATE)
@Table(
    name = "feature_tables",
    uniqueConstraints = @UniqueConstraint(columnNames = {"name", "project_name"}))
@NamedEntityGraph(
    name = "FeatureTable.attributes",
    attributeNodes = {
      @NamedAttributeNode("entities"),
      @NamedAttributeNode("features"),
      @NamedAttributeNode("streamSource"),
      @NamedAttributeNode("batchSource"),
      @NamedAttributeNode("onlineStore")
    })
public class FeatureTable extends AbstractTimestampEntity {

  @Id @GeneratedValue private long id;

  // Name of Feature Table
  @Column(name = "name", nullable = false)
  private String name;

  // Name of the Project that this FeatureTable belongs to
  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "project_name")
  private Project project;

  // Features defined in this Feature Table
  @OneToMany(
      mappedBy = "featureTable",
      cascade = CascadeType.ALL,
      fetch = FetchType.LAZY,
      orphanRemoval = true)
  private Set<Feature> features;

  // Entities to associate the features defined in this FeatureTable with
  @ManyToMany(fetch = FetchType.LAZY)
  @JoinTable(
      name = "feature_tables_entities",
      joinColumns = @JoinColumn(name = "feature_table_id"),
      inverseJoinColumns = @JoinColumn(name = "entity_id"))
  private Set<Entity> entities;

  // User defined metadata labels serialized as JSON string.
  @Column(name = "labels", columnDefinition = "text")
  private String labelsJSON;

  // Max Age of the Features defined in this Feature Table in seconds
  @Column(name = "max_age", nullable = false)
  private long maxAgeSecs;

  // Streaming DataSource used to obtain data for features from a stream
  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "stream_source_id", nullable = true)
  private DataSource streamSource;

  // Batched DataSource used to obtain data for features from a batch of data
  @OneToOne(cascade = CascadeType.ALL)
  @JoinColumn(name = "batch_source_id", nullable = false)
  private DataSource batchSource;

  // Auto-incrementing version no. of this FeatureTable.
  // Auto-increments every update made to the FeatureTable.
  @Column(name = "revision", nullable = false)
  private int revision;

  @Column(name = "is_deleted", nullable = false)
  private boolean isDeleted;

  // Name of the online store where FeatureTable is stored
  @ManyToOne(optional = true, fetch = FetchType.LAZY)
  @JoinColumn(name = "online_store_name")
  private OnlineStore onlineStore;

  public FeatureTable() {}

  /**
   * Construct FeatureTable from Protobuf spec representation in the given project with entities
   * registered in entity repository.
   *
   * @param projectName the name of the project that the constructed FeatureTable belongs.
   * @param spec the Protobuf spec to construct the Feature from.
   * @param entityRepo {@link EntityRepository} used to resolve entity names.
   * @throws IllegalArgumentException if the Protobuf spec provided is invalid.
   * @return constructed FeatureTable from the given Protobuf spec.
   */
  public static FeatureTable fromProto(
      String projectName, FeatureTableSpec spec, EntityRepository entityRepo, Long defaultMaxAgeSeconds) {
    FeatureTable table = new FeatureTable();
    table.setName(spec.getName());
    table.setProject(new Project(projectName));

    Set<Feature> features =
        spec.getFeaturesList().stream()
            .map(featureSpec -> Feature.fromProto(table, featureSpec))
            .collect(Collectors.toSet());
    table.setFeatures(features);

    Set<Entity> entities =
        FeatureTable.resolveEntities(
            projectName, spec.getName(), entityRepo, spec.getEntitiesList());
    table.setEntities(entities);

    String labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());
    table.setLabelsJSON(labelsJSON);

    Long maxAge = defaultMaxAgeSeconds;
    if (spec.hasMaxAge()) {
      maxAge = spec.getMaxAge().getSeconds();
    }

    table.setMaxAgeSecs(maxAge);
    table.setBatchSource(DataSource.fromProto(spec.getBatchSource()));
    table.setOnlineStore(OnlineStore.fromProto(spec.getOnlineStore()));

    // Configure stream source only if set
    if (!spec.getStreamSource().equals(DataSourceProto.DataSource.getDefaultInstance())) {
      table.setStreamSource(DataSource.fromProto(spec.getStreamSource()));
    }

    return table;
  }

  /**
   * Update the FeatureTable from the given Protobuf representation.
   *
   * @param spec the Protobuf spec to update the FeatureTable from.
   * @throws IllegalArgumentException if the update will make prohibited changes.
   */
  public void updateFromProto(
      String projectName, FeatureTableSpec spec, EntityRepository entityRepo, Long defaultMaxAgeSeconds) {
    // Check for prohibited changes made in spec:
    // - Name cannot be changed
    if (!getName().equals(spec.getName())) {
      throw new IllegalArgumentException(
          String.format(
              "Updating the name of a registered FeatureTable is not allowed: %s to %s",
              getName(), spec.getName()));
    }
    // Update Entities if changed
    Set<Entity> entities =
        FeatureTable.resolveEntities(
            projectName, spec.getName(), entityRepo, spec.getEntitiesList());
    this.setEntities(entities);

    // Update FeatureTable based on spec
    // Update existing features, create new feature, drop missing features
    Map<String, Feature> existingFeatures =
        getFeatures().stream().collect(Collectors.toMap(Feature::getName, feature -> feature));
    this.features.clear();
    this.features.addAll(
        spec.getFeaturesList().stream()
            .map(
                featureSpec -> {
                  if (!existingFeatures.containsKey(featureSpec.getName())) {
                    // Create new Feature based on spec
                    return Feature.fromProto(this, featureSpec);
                  }
                  // Update existing feature based on spec
                  Feature feature = existingFeatures.get(featureSpec.getName());
                  feature.updateFromProto(featureSpec);
                  return feature;
                })
            .collect(Collectors.toSet()));


    if (spec.hasMaxAge()) {
      this.maxAgeSecs = spec.getMaxAge().getSeconds();
    }
    
    this.labelsJSON = TypeConversion.convertMapToJsonString(spec.getLabelsMap());

    this.batchSource = DataSource.fromProto(spec.getBatchSource());
    if (!spec.getStreamSource().equals(DataSourceProto.DataSource.getDefaultInstance())) {
      this.streamSource = DataSource.fromProto(spec.getStreamSource());
    } else {
      this.streamSource = null;
    }
    this.setOnlineStore(OnlineStore.fromProto(spec.getOnlineStore()));

    // Set isDeleted to false
    this.setDeleted(false);

    // Bump revision no.
    this.revision++;
  }

  /** Convert this Feature Table to its Protobuf representation */
  public FeatureTableProto.FeatureTable toProto() {
    // Convert field types to Protobuf compatible types
    Timestamp creationTime = TypeConversion.convertTimestamp(getCreated());
    Timestamp updatedTime = TypeConversion.convertTimestamp(getLastUpdated());
    String metadataHashBytes = this.protoHash();

    List<FeatureProto.FeatureSpec> featureSpecs =
        getFeatures().stream().map(Feature::toProto).collect(Collectors.toList());
    List<String> entityNames =
        getEntities().stream().map(Entity::getName).collect(Collectors.toList());
    Map<String, String> labels = TypeConversion.convertJsonStringToMap(getLabelsJSON());

    FeatureTableSpec.Builder spec =
        FeatureTableSpec.newBuilder()
            .setName(getName())
            .setMaxAge(Duration.newBuilder().setSeconds(getMaxAgeSecs()).build())
            .setBatchSource(getBatchSource().toProto())
            .addAllEntities(entityNames)
            .addAllFeatures(featureSpecs)
            .putAllLabels(labels)
            .setOnlineStore(getOnlineStore().toProto());
    if (getStreamSource() != null) {
      spec.setStreamSource(getStreamSource().toProto());
    }

    return FeatureTableProto.FeatureTable.newBuilder()
        .setMeta(
            FeatureTableProto.FeatureTableMeta.newBuilder()
                .setRevision(getRevision())
                .setCreatedTimestamp(creationTime)
                .setLastUpdatedTimestamp(updatedTime)
                .setHash(metadataHashBytes)
                .build())
        .setSpec(spec.build())
        .build();
  }

  /** Use given entity repository to resolve entity names to entity native objects */
  private static Set<Entity> resolveEntities(
      String projectName, String tableName, EntityRepository repo, Collection<String> names) {
    return names.stream()
        .map(
            entityName -> {
              Entity entity = repo.findEntityByNameAndProject_Name(entityName, projectName);
              if (entity == null) {
                throw new IllegalArgumentException(
                    String.format(
                        "Feature Table refers to no existent Entity: (table: %s, entity: %s, project: %s)",
                        tableName, entityName, projectName));
              }
              return entity;
            })
        .collect(Collectors.toSet());
  }

  /**
   * Return a boolean to indicate if FeatureTable contains all specified entities.
   *
   * @param entitiesFilter contain entities that should be attached to the FeatureTable
   * @return boolean True if FeatureTable contains all entities in the entitiesFilter
   */
  public boolean hasAllEntities(List<String> entitiesFilter) {
    Set<String> allEntitiesName =
        this.getEntities().stream().map(Entity::getName).collect(Collectors.toSet());
    return allEntitiesName.equals(new HashSet<>(entitiesFilter));
  }

  /**
   * Returns a map of Feature references and Features if FeatureTable's Feature contains all labels
   * in the labelsFilter
   *
   * @param labelsFilter contain labels that should be attached to FeatureTable's features
   * @return Map of Feature references and Features
   */
  public Map<String, Feature> getFeaturesByLabels(Map<String, String> labelsFilter) {
    Map<String, Feature> validFeaturesMap;
    List<Feature> validFeatures;
    if (labelsFilter.size() > 0) {
      validFeatures = filterFeaturesByAllLabels(this.getFeatures(), labelsFilter);
      validFeaturesMap = getFeaturesRefToFeaturesMap(validFeatures);
      return validFeaturesMap;
    }
    validFeaturesMap = getFeaturesRefToFeaturesMap(List.copyOf(this.getFeatures()));
    return validFeaturesMap;
  }

  /**
   * Returns map for accessing features using their respective feature reference.
   *
   * @param features List of features to insert to map.
   * @return Map of featureRef:feature.
   */
  private Map<String, Feature> getFeaturesRefToFeaturesMap(List<Feature> features) {
    Map<String, Feature> validFeaturesMap = new HashMap<>();
    for (Feature feature : features) {
      validFeaturesMap.put(String.format("%s:%s", this.name, feature.getName()), feature);
    }
    return validFeaturesMap;
  }

  /**
   * Returns a list of Features if FeatureTable's Feature contains all labels in labelsFilter
   *
   * @param labelsFilter contain labels that should be attached to FeatureTable's features
   * @return List of Features
   */
  public static List<Feature> filterFeaturesByAllLabels(
      Set<Feature> features, Map<String, String> labelsFilter) {
    List<Feature> validFeatures =
        features.stream()
            .filter(feature -> feature.hasAllLabels(labelsFilter))
            .collect(Collectors.toList());

    return validFeatures;
  }

  /**
   * Determine whether a FeatureTable has all the specified labels.
   *
   * @param labelsFilter labels contain key-value mapping for labels attached to the FeatureTable
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

  public void setOnlineStore(OnlineStore onlineStore) {
    this.onlineStore = onlineStore;
  }

  public Map<String, String> getLabelsMap() {
    return TypeConversion.convertJsonStringToMap(getLabelsJSON());
  }

  public void delete() {
    this.setDeleted(true);
    this.setRevision(0);
  }

  public String protoHash() {
    List<String> sortedEntities =
        this.getEntities().stream().map(Entity::getName).sorted().collect(Collectors.toList());

    List<FeatureProto.FeatureSpec> sortedFeatureSpecs =
        this.getFeatures().stream()
            .sorted(Comparator.comparing(Feature::getName))
            .map(Feature::toProto)
            .collect(Collectors.toList());

    DataSourceProto.DataSource streamSource = DataSourceProto.DataSource.getDefaultInstance();
    if (getStreamSource() != null) {
      streamSource = getStreamSource().toProto();
    }

    FeatureTableSpec featureTableSpec =
        FeatureTableSpec.newBuilder()
            .addAllEntities(sortedEntities)
            .addAllFeatures(sortedFeatureSpecs)
            .setBatchSource(getBatchSource().toProto())
            .setStreamSource(streamSource)
            .setOnlineStore(getOnlineStore().toProto())
            .setMaxAge(Duration.newBuilder().setSeconds(getMaxAgeSecs()).build())
            .build();
    return Hashing.murmur3_32().hashBytes(featureTableSpec.toByteArray()).toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getName(),
        getProject(),
        getFeatures(),
        getEntities(),
        getMaxAgeSecs(),
        getBatchSource(),
        getStreamSource(),
        getOnlineStore());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FeatureTable)) {
      return false;
    }

    FeatureTable other = (FeatureTable) o;

    return getName().equals(other.getName())
        && getProject().equals(other.getProject())
        && getLabelsJSON().equals(other.getLabelsJSON())
        && getFeatures().equals(other.getFeatures())
        && getEntities().equals(other.getEntities())
        && getOnlineStore().equals(other.getOnlineStore())
        && getMaxAgeSecs() == other.getMaxAgeSecs()
        && Optional.ofNullable(getBatchSource()).equals(Optional.ofNullable(other.getBatchSource()))
        && Optional.ofNullable(getStreamSource())
            .equals(Optional.ofNullable(other.getStreamSource()));
  }
}
