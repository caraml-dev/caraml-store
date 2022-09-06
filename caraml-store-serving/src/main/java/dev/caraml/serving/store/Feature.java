package dev.caraml.serving.store;

import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import java.util.HashMap;

public interface Feature {

  HashMap<ValueType.Enum, Value.ValCase> TYPE_TO_VAL_CASE =
      new HashMap() {
        {
          put(ValueType.Enum.BYTES, Value.ValCase.BYTES_VAL);
          put(ValueType.Enum.STRING, Value.ValCase.STRING_VAL);
          put(ValueType.Enum.INT32, Value.ValCase.INT32_VAL);
          put(ValueType.Enum.INT64, Value.ValCase.INT64_VAL);
          put(ValueType.Enum.DOUBLE, Value.ValCase.DOUBLE_VAL);
          put(ValueType.Enum.FLOAT, Value.ValCase.FLOAT_VAL);
          put(ValueType.Enum.BOOL, Value.ValCase.BOOL_VAL);
          put(ValueType.Enum.BYTES_LIST, Value.ValCase.BYTES_LIST_VAL);
          put(ValueType.Enum.STRING_LIST, Value.ValCase.STRING_LIST_VAL);
          put(ValueType.Enum.INT32_LIST, Value.ValCase.INT32_LIST_VAL);
          put(ValueType.Enum.INT64_LIST, Value.ValCase.INT64_LIST_VAL);
          put(ValueType.Enum.DOUBLE_LIST, Value.ValCase.DOUBLE_LIST_VAL);
          put(ValueType.Enum.FLOAT_LIST, Value.ValCase.FLOAT_LIST_VAL);
          put(ValueType.Enum.BOOL_LIST, Value.ValCase.BOOL_LIST_VAL);
        }
      };

  Value getFeatureValue(ValueType.Enum valueType);

  FeatureReference getFeatureReference();

  Timestamp getEventTimestamp();
}
