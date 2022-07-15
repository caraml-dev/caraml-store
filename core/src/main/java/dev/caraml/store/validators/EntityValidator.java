package dev.caraml.store.validators;

import dev.caraml.store.protobuf.core.EntityProto;

public class EntityValidator {
  public static void validateSpec(EntityProto.EntitySpec entitySpec) {
    if (entitySpec.getName().isEmpty()) {
      throw new IllegalArgumentException("Entity name must be provided");
    }
    if (entitySpec.getValueType().toString().isEmpty()) {
      throw new IllegalArgumentException("Entity type must not be empty");
    }
    if (entitySpec.getLabelsMap().containsKey("")) {
      throw new IllegalArgumentException("Entity label keys must not be empty");
    }

    Matchers.checkValidCharacters(entitySpec.getName(), "entity");
  }
}
