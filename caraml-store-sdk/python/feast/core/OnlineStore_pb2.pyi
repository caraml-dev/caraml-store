from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

BIGTABLE: StoreType
DESCRIPTOR: _descriptor.FileDescriptor
REDIS: StoreType
UNSET: StoreType

class OnlineStore(_message.Message):
    __slots__ = ["description", "name", "type"]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    description: str
    name: str
    type: StoreType
    def __init__(self, name: _Optional[str] = ..., type: _Optional[_Union[StoreType, str]] = ..., description: _Optional[str] = ...) -> None: ...

class StoreType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
