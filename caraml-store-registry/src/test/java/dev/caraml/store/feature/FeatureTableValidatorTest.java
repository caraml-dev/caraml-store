package dev.caraml.store.feature;

import static dev.caraml.store.protobuf.types.ValueProto.ValueType.Enum.INT64;
import static dev.caraml.store.protobuf.types.ValueProto.ValueType.Enum.STRING;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.testutils.it.DataGenerator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class FeatureTableValidatorTest {

  @Test
  public void shouldErrorIfLabelsHasEmptyKey() {
    Map<String, String> badLabels = Map.of("", "empty");
    FeatureTableSpec badSpec = getTestSpec().toBuilder().putAllLabels(badLabels).build();
    assertThrows(IllegalArgumentException.class, () -> FeatureTableValidator.validateSpec(badSpec));
  }

  @Test
  public void shouldErrorIfFeaturesLabelsHasEmptyKey() {
    Map<String, String> badLabels = Map.of("", "empty");

    List<FeatureSpec> badFeatureSpecs =
        getTestSpec().getFeaturesList().stream()
            .map(featureSpec -> featureSpec.toBuilder().putAllLabels(badLabels).build())
            .collect(Collectors.toList());
    FeatureTableSpec badSpec = getTestSpec().toBuilder().addAllFeatures(badFeatureSpecs).build();
    assertThrows(IllegalArgumentException.class, () -> FeatureTableValidator.validateSpec(badSpec));
  }

  @Test
  public void shouldErrorIfUsedReservedName() {
    FeatureTableSpec badSpec =
        getTestSpec().toBuilder().addAllEntities(FeatureTableValidator.RESERVED_NAMES).build();
    assertThrows(IllegalArgumentException.class, () -> FeatureTableValidator.validateSpec(badSpec));
  }

  @Test
  public void shouldErrorIfNamesUsedNotUnique() {
    FeatureTableSpec badSpec =
        DataGenerator.createFeatureTableSpec(
            "driver", List.of("region"), Map.of("region", STRING), 3600, Map.of());
    assertThrows(IllegalArgumentException.class, () -> FeatureTableValidator.validateSpec(badSpec));
  }

  private FeatureTableSpec getTestSpec() {
    return DataGenerator.createFeatureTableSpec(
        "driver", List.of("driver_id"), Map.of("n_drivers", INT64), 3600, Map.of());
  }
}
