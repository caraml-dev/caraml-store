from feast.types import Value_pb2 as _Value_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Field(_message.Message):
    __slots__ = ["name", "value"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: _Value_pb2.Value
    def __init__(self, name: _Optional[str] = ..., value: _Optional[_Union[_Value_pb2.Value, _Mapping]] = ...) -> None: ...
