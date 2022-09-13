package dev.caraml.serving.store;

import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;

public class ProtoFeature implements Feature {
  private final FeatureReference featureReference;

  private final Timestamp eventTimestamp;

  private final Value featureValue;

  public ProtoFeature(
      FeatureReference featureReference, Timestamp eventTimestamp, Value featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  /**
   * Returns Feast valueType if type matches, otherwise null.
   *
   * @param valueType Feast valueType of feature as specified in FeatureSpec
   * @return Value representation of feature
   */
  @Override
  public Value getFeatureValue(ValueType.Enum valueType) {
    if (TYPE_TO_VAL_CASE.get(valueType) != this.featureValue.getValCase()) {
      return null;
    }

    return this.featureValue;
  }

  @Override
  public FeatureReference getFeatureReference() {
    return this.featureReference;
  }

  @Override
  public Timestamp getEventTimestamp() {
    return this.eventTimestamp;
  }
}
