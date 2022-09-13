package dev.caraml.serving.store;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto.BoolList;
import dev.caraml.store.protobuf.types.ValueProto.BytesList;
import dev.caraml.store.protobuf.types.ValueProto.DoubleList;
import dev.caraml.store.protobuf.types.ValueProto.FloatList;
import dev.caraml.store.protobuf.types.ValueProto.Int32List;
import dev.caraml.store.protobuf.types.ValueProto.Int64List;
import dev.caraml.store.protobuf.types.ValueProto.StringList;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

public class AvroFeature implements Feature {
  private final FeatureReference featureReference;

  private final Timestamp eventTimestamp;

  private final Object featureValue;

  public AvroFeature(
      FeatureReference featureReference, Timestamp eventTimestamp, Object featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  /**
   * Casts feature value of Object type based on Feast valueType. Empty object i.e new Object() is
   * interpreted as VAL_NOT_SET Feast valueType.
   *
   * @param valueType Feast valueType of feature as specified in FeatureSpec
   * @return ValueProto.Value representation of feature
   */
  @Override
  public Value getFeatureValue(ValueType.Enum valueType) {
    Value finalValue;

    try {
      switch (valueType) {
        case STRING:
          finalValue = Value.newBuilder().setStringVal(((Utf8) featureValue).toString()).build();
          break;
        case INT32:
          finalValue = Value.newBuilder().setInt32Val((Integer) featureValue).build();
          break;
        case INT64:
          finalValue = Value.newBuilder().setInt64Val((Long) featureValue).build();
          break;
        case DOUBLE:
          finalValue = Value.newBuilder().setDoubleVal((Double) featureValue).build();
          break;
        case FLOAT:
          finalValue = Value.newBuilder().setFloatVal((Float) featureValue).build();
          break;
        case BYTES:
          finalValue =
              Value.newBuilder()
                  .setBytesVal(ByteString.copyFrom(((ByteBuffer) featureValue).array()))
                  .build();
          break;
        case BOOL:
          finalValue = Value.newBuilder().setBoolVal((Boolean) featureValue).build();
          break;
        case STRING_LIST:
          finalValue =
              Value.newBuilder()
                  .setStringListVal(
                      StringList.newBuilder()
                          .addAllVal(
                              ((GenericData.Array<Utf8>) featureValue)
                                  .stream().map(Utf8::toString).collect(Collectors.toList()))
                          .build())
                  .build();
          break;
        case INT64_LIST:
          finalValue =
              Value.newBuilder()
                  .setInt64ListVal(
                      Int64List.newBuilder()
                          .addAllVal(((GenericData.Array<Long>) featureValue))
                          .build())
                  .build();
          break;
        case INT32_LIST:
          finalValue =
              Value.newBuilder()
                  .setInt32ListVal(
                      Int32List.newBuilder()
                          .addAllVal(((GenericData.Array<Integer>) featureValue))
                          .build())
                  .build();
          break;
        case FLOAT_LIST:
          finalValue =
              Value.newBuilder()
                  .setFloatListVal(
                      FloatList.newBuilder()
                          .addAllVal(((GenericData.Array<Float>) featureValue))
                          .build())
                  .build();
          break;
        case DOUBLE_LIST:
          finalValue =
              Value.newBuilder()
                  .setDoubleListVal(
                      DoubleList.newBuilder()
                          .addAllVal(((GenericData.Array<Double>) featureValue))
                          .build())
                  .build();
          break;
        case BOOL_LIST:
          finalValue =
              Value.newBuilder()
                  .setBoolListVal(
                      BoolList.newBuilder()
                          .addAllVal(((GenericData.Array<Boolean>) featureValue))
                          .build())
                  .build();
          break;
        case BYTES_LIST:
          finalValue =
              Value.newBuilder()
                  .setBytesListVal(
                      BytesList.newBuilder()
                          .addAllVal(
                              ((GenericData.Array<ByteBuffer>) featureValue)
                                  .stream()
                                      .map(byteBuffer -> ByteString.copyFrom(byteBuffer.array()))
                                      .collect(Collectors.toList()))
                          .build())
                  .build();
          break;
        default:
          throw new RuntimeException("FeatureType is not supported");
      }
    } catch (ClassCastException e) {
      // Feature type has changed
      finalValue = Value.newBuilder().build();
    }

    return finalValue;
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
