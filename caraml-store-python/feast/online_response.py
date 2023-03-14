from typing import List, Dict, Any, cast, Type, Union

from google.protobuf.json_format import MessageToDict

from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.types.Value_pb2 import Value, ValueType


class OnlineResponse:
    """
    Defines a online response in feast.
    """

    def __init__(self, online_response_proto: GetOnlineFeaturesResponse):
        """
        Construct a native online response from its protobuf version.
        Args:
        online_response_proto: GetOnlineResponse proto object to construct from.
        """
        self.proto = online_response_proto

    @property
    def rows(self):
        """
        Getter for GetOnlineResponse's field_values.
        """
        return self.proto.results

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary form.
        """
        field_names = [field_name for field_name in self.proto.metadata.field_names.val]
        features_dict: Dict[str, List[Any]] = {k: list() for k in field_names}

        for row in self.rows:
            for field_name, value in zip(field_names, row.values):
                native_type_value = _feast_value_type_to_python_type(value)
                features_dict[field_name].append(native_type_value)
        return features_dict


def infer_online_entity_rows(
    entity_rows: List[Dict[str, Any]]
) -> List[GetOnlineFeaturesRequest.EntityRow]:
    """
    Builds a list of EntityRow protos from Python native type format passed by user.
    Args:
        entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
    Returns:
        A list of EntityRow protos parsed from args.
    """

    entity_rows_dicts = cast(List[Dict[str, Any]], entity_rows)
    entity_row_list = []
    entity_type_map = dict()

    for entity in entity_rows_dicts:
        fields = {}
        for key, value in entity.items():
            # Allow for feast.types.Value
            if isinstance(value, Value):
                proto_value = value
            else:
                # Infer the specific type for this row
                current_dtype = _python_type_to_entity_value_type(type(value))

                if key not in entity_type_map:
                    entity_type_map[key] = current_dtype
                else:
                    if current_dtype != entity_type_map[key]:
                        raise TypeError(
                            f"Input entity {key} has mixed types, {current_dtype} and {entity_type_map[key]}. That is not allowed. "
                        )
                proto_value = _python_value_to_entity_value(current_dtype, value)
            fields[key] = proto_value
        entity_row_list.append(GetOnlineFeaturesRequest.EntityRow(fields=fields))
    return entity_row_list


def _python_type_to_entity_value_type(python_type: Type) -> ValueType:
    """
    Finds the equivalent Feast Value Type for a Python value. Only types applicable to an Entity are supported here.
    Args:
        value: Python primitive type
    Returns:
        Feast Value Type
    """

    type_map: Dict[Type, ValueType] = {
        int: ValueType.INT64,
        str: ValueType.STRING,
        bool: ValueType.BOOL,
    }
    return type_map[python_type]


def _python_value_to_entity_value(value_type: ValueType, value):
    if value_type == ValueType.INT64:
        return Value(int64_val=value)
    elif value_type == ValueType.BOOL:
        return Value(bool_val=value)
    else:
        return Value(string_val=value)


def _feast_value_type_to_python_type(field_value_proto: Value) -> Any:
    """
    Converts field value Proto to Dict and returns each field's Feast Value Type value
    in their respective Python value.
    Args:
        field_value_proto: Field value Proto
    Returns:
        Python native type representation/version of the given field_value_proto
    """
    field_value_dict = MessageToDict(field_value_proto)

    for k, v in field_value_dict.items():
        if k == "int64Val":
            return int(v)
        if k == "bytesVal":
            return bytes(v)
        if (k == "int64ListVal") or (k == "int32ListVal"):
            return [int(item) for item in v["val"]]
        if (k == "floatListVal") or (k == "doubleListVal"):
            return [float(item) for item in v["val"]]
        if k == "stringListVal":
            return [str(item) for item in v["val"]]
        if k == "bytesListVal":
            return [bytes(item) for item in v["val"]]
        if k == "boolListVal":
            return [bool(item) for item in v["val"]]

        if k in ["int32Val", "floatVal", "doubleVal", "stringVal", "boolVal"]:
            return v
        else:
            raise TypeError(
                f"Casting to Python native type for type {k} failed. "
                f"Type {k} not found"
            )
