from feast.types import Value_pb2 as _Value_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Entity(_message.Message):
    __slots__ = ["meta", "spec"]
    META_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    meta: EntityMeta
    spec: EntitySpec
    def __init__(self, spec: _Optional[_Union[EntitySpec, _Mapping]] = ..., meta: _Optional[_Union[EntityMeta, _Mapping]] = ...) -> None: ...

class EntityMeta(_message.Message):
    __slots__ = ["created_timestamp", "last_updated_timestamp"]
    CREATED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    created_timestamp: _timestamp_pb2.Timestamp
    last_updated_timestamp: _timestamp_pb2.Timestamp
    def __init__(self, created_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., last_updated_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class EntitySpec(_message.Message):
    __slots__ = ["description", "labels", "name", "value_type"]
    class LabelsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    description: str
    labels: _containers.ScalarMap[str, str]
    name: str
    value_type: _Value_pb2.ValueType.Enum
    def __init__(self, name: _Optional[str] = ..., value_type: _Optional[_Union[_Value_pb2.ValueType.Enum, str]] = ..., description: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...
