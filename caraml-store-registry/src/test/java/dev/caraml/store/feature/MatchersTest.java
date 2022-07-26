package dev.caraml.store.feature;

import static dev.caraml.store.feature.Matchers.checkLowerSnakeCase;
import static dev.caraml.store.feature.Matchers.checkUpperSnakeCase;
import static dev.caraml.store.feature.Matchers.checkValidClassPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

public class MatchersTest {

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCase() {
    String in = "REDIS_DB";
    checkUpperSnakeCase(in, "featuretable");
  }

  @Test
  public void checkUpperSnakeCaseShouldPassForLegitUpperSnakeCaseWithNumbers() {
    String in = "REDIS1";
    checkUpperSnakeCase(in, "featuretable");
  }

  @Test
  public void checkUpperSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    String in = "redis";

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> checkUpperSnakeCase(in, "featuretable"));
    assertEquals(
        exception.getMessage(),
        Strings.lenientFormat(
            "invalid value for %s resource, %s: %s",
            "featuretable",
            "redis",
            "argument must be in upper snake case, and cannot include any special characters."));
  }

  @Test
  public void checkLowerSnakeCaseShouldPassForLegitLowerSnakeCase() {
    String in = "feature_name_v1";
    checkLowerSnakeCase(in, "feature");
  }

  @Test
  public void checkLowerSnakeCaseShouldThrowIllegalArgumentExceptionWithFieldForInvalidString() {
    String in = "Invalid_feature name";
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> checkLowerSnakeCase(in, "feature"));
    assertEquals(
        exception.getMessage(),
        Strings.lenientFormat(
            "invalid value for %s resource, %s: %s",
            "feature",
            "Invalid_feature name",
            "argument must be in lower snake case, and cannot include any special characters."));
  }

  @Test
  public void checkValidClassPathSuccess() {
    checkValidClassPath("com.example.foo", "FeatureTable");
    checkValidClassPath("com.example", "FeatureTable");
  }

  @Test
  public void checkValidClassPathEmpty() {
    assertThrows(IllegalArgumentException.class, () -> checkValidClassPath("", "FeatureTable"));
  }

  @Test
  public void checkValidClassPathDigits() {
    assertThrows(IllegalArgumentException.class, () -> checkValidClassPath("123", "FeatureTable"));
  }
}
