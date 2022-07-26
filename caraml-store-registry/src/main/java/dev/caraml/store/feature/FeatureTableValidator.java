package dev.caraml.store.feature;

import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType;
import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

public class FeatureTableValidator {
  protected static final Set<String> RESERVED_NAMES =
      Set.of("created_timestamp", "event_timestamp");

  public static void validateSpec(FeatureTableSpec spec) {
    if (spec.getName().isEmpty()) {
      throw new IllegalArgumentException("FeatureTable name must be provided");
    }
    if (spec.getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("FeatureTable cannot have labels with empty key.");
    }
    if (spec.getEntitiesCount() == 0) {
      throw new IllegalArgumentException("FeatureTable entities list cannot be empty.");
    }
    if (spec.getFeaturesCount() == 0) {
      throw new IllegalArgumentException("FeatureTable features list cannot be empty.");
    }
    if (!spec.hasBatchSource()) {
      throw new IllegalArgumentException("FeatureTable batch source cannot be empty.");
    }

    Matchers.checkValidCharacters(spec.getName(), "FeatureTable");
    spec.getFeaturesList().forEach(FeatureTableValidator::validateFeatureSpec);

    // Check that features and entities defined in FeatureTable do not use reserved names
    ArrayList<String> fieldNames = new ArrayList<>(spec.getEntitiesList());
    fieldNames.addAll(spec.getFeaturesList().stream().map(FeatureSpec::getName).toList());
    if (!Collections.disjoint(fieldNames, RESERVED_NAMES)) {
      throw new IllegalArgumentException(
          String.format(
              "Reserved names has been used as Feature(s) names. Reserved: %s", RESERVED_NAMES));
    }

    // Check that Feature and Entity names in FeatureTable do not collide with each other
    if (Matchers.hasDuplicates(fieldNames)) {
      throw new IllegalArgumentException(
          "Entity and Feature names within a Feature Table should be unique.");
    }

    // Check that the data sources defined in the feature table are valid
    if (!spec.getBatchSource().getType().equals(SourceType.INVALID)) {
      DataSourceValidator.validate(spec.getBatchSource());
    }
    if (!spec.getStreamSource().getType().equals(SourceType.INVALID)) {
      DataSourceValidator.validate(spec.getStreamSource());
    }
  }

  private static void validateFeatureSpec(FeatureSpec spec) {
    Matchers.checkValidCharacters(spec.getName(), "Feature");
    if (spec.getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("Features cannot have labels with empty key.");
    }
  }
}
