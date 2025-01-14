from google.protobuf import timestamp_pb2 as _timestamp_pb2
from feast.types import Value_pb2 as _Value_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
INGESTION_FAILURE: FieldStatus
INVALID: FieldStatus
NOT_FOUND: FieldStatus
NULL_VALUE: FieldStatus
OUTSIDE_MAX_AGE: FieldStatus
PRESENT: FieldStatus

class FeatureReference(_message.Message):
    __slots__ = ["feature_table", "name"]
    FEATURE_TABLE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    feature_table: str
    name: str
    def __init__(self, feature_table: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class FieldList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, val: _Optional[_Iterable[str]] = ...) -> None: ...

class GetFeastServingInfoRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetFeastServingInfoResponse(_message.Message):
    __slots__ = ["version"]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    def __init__(self, version: _Optional[str] = ...) -> None: ...

class GetOnlineFeaturesRequest(_message.Message):
    __slots__ = ["entity_rows", "features", "project"]
    class EntityRow(_message.Message):
        __slots__ = ["fields", "timestamp"]
        class FieldsEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: _Value_pb2.Value
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_Value_pb2.Value, _Mapping]] = ...) -> None: ...
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        fields: _containers.MessageMap[str, _Value_pb2.Value]
        timestamp: _timestamp_pb2.Timestamp
        def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., fields: _Optional[_Mapping[str, _Value_pb2.Value]] = ...) -> None: ...
    ENTITY_ROWS_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    entity_rows: _containers.RepeatedCompositeFieldContainer[GetOnlineFeaturesRequest.EntityRow]
    features: _containers.RepeatedCompositeFieldContainer[FeatureReference]
    project: str
    def __init__(self, features: _Optional[_Iterable[_Union[FeatureReference, _Mapping]]] = ..., entity_rows: _Optional[_Iterable[_Union[GetOnlineFeaturesRequest.EntityRow, _Mapping]]] = ..., project: _Optional[str] = ...) -> None: ...

class GetOnlineFeaturesResponse(_message.Message):
    __slots__ = ["metadata", "results"]
    class FieldVector(_message.Message):
        __slots__ = ["statuses", "values"]
        STATUSES_FIELD_NUMBER: _ClassVar[int]
        VALUES_FIELD_NUMBER: _ClassVar[int]
        statuses: _containers.RepeatedScalarFieldContainer[FieldStatus]
        values: _containers.RepeatedCompositeFieldContainer[_Value_pb2.Value]
        def __init__(self, values: _Optional[_Iterable[_Union[_Value_pb2.Value, _Mapping]]] = ..., statuses: _Optional[_Iterable[_Union[FieldStatus, str]]] = ...) -> None: ...
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    metadata: GetOnlineFeaturesResponseMetadata
    results: _containers.RepeatedCompositeFieldContainer[GetOnlineFeaturesResponse.FieldVector]
    def __init__(self, metadata: _Optional[_Union[GetOnlineFeaturesResponseMetadata, _Mapping]] = ..., results: _Optional[_Iterable[_Union[GetOnlineFeaturesResponse.FieldVector, _Mapping]]] = ...) -> None: ...

class GetOnlineFeaturesResponseMetadata(_message.Message):
    __slots__ = ["field_names"]
    FIELD_NAMES_FIELD_NUMBER: _ClassVar[int]
    field_names: FieldList
    def __init__(self, field_names: _Optional[_Union[FieldList, _Mapping]] = ...) -> None: ...

class GetOnlineFeaturesResponseV2(_message.Message):
    __slots__ = ["field_values"]
    class FieldValues(_message.Message):
        __slots__ = ["fields", "statuses"]
        class FieldsEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: _Value_pb2.Value
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_Value_pb2.Value, _Mapping]] = ...) -> None: ...
        class StatusesEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: FieldStatus
            def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FieldStatus, str]] = ...) -> None: ...
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        STATUSES_FIELD_NUMBER: _ClassVar[int]
        fields: _containers.MessageMap[str, _Value_pb2.Value]
        statuses: _containers.ScalarMap[str, FieldStatus]
        def __init__(self, fields: _Optional[_Mapping[str, _Value_pb2.Value]] = ..., statuses: _Optional[_Mapping[str, FieldStatus]] = ...) -> None: ...
    FIELD_VALUES_FIELD_NUMBER: _ClassVar[int]
    field_values: _containers.RepeatedCompositeFieldContainer[GetOnlineFeaturesResponseV2.FieldValues]
    def __init__(self, field_values: _Optional[_Iterable[_Union[GetOnlineFeaturesResponseV2.FieldValues, _Mapping]]] = ...) -> None: ...

class FieldStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
