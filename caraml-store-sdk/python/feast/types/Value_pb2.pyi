from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BoolList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, val: _Optional[_Iterable[bool]] = ...) -> None: ...

class BytesList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, val: _Optional[_Iterable[bytes]] = ...) -> None: ...

class DoubleList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, val: _Optional[_Iterable[float]] = ...) -> None: ...

class FloatList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, val: _Optional[_Iterable[float]] = ...) -> None: ...

class Int32List(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, val: _Optional[_Iterable[int]] = ...) -> None: ...

class Int64List(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, val: _Optional[_Iterable[int]] = ...) -> None: ...

class StringList(_message.Message):
    __slots__ = ["val"]
    VAL_FIELD_NUMBER: _ClassVar[int]
    val: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, val: _Optional[_Iterable[str]] = ...) -> None: ...

class Value(_message.Message):
    __slots__ = ["bool_list_val", "bool_val", "bytes_list_val", "bytes_val", "double_list_val", "double_val", "float_list_val", "float_val", "int32_list_val", "int32_val", "int64_list_val", "int64_val", "string_list_val", "string_val"]
    BOOL_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    BOOL_VAL_FIELD_NUMBER: _ClassVar[int]
    BYTES_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    BYTES_VAL_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VAL_FIELD_NUMBER: _ClassVar[int]
    FLOAT_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VAL_FIELD_NUMBER: _ClassVar[int]
    INT32_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    INT32_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_VAL_FIELD_NUMBER: _ClassVar[int]
    STRING_LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    STRING_VAL_FIELD_NUMBER: _ClassVar[int]
    bool_list_val: BoolList
    bool_val: bool
    bytes_list_val: BytesList
    bytes_val: bytes
    double_list_val: DoubleList
    double_val: float
    float_list_val: FloatList
    float_val: float
    int32_list_val: Int32List
    int32_val: int
    int64_list_val: Int64List
    int64_val: int
    string_list_val: StringList
    string_val: str
    def __init__(self, bytes_val: _Optional[bytes] = ..., string_val: _Optional[str] = ..., int32_val: _Optional[int] = ..., int64_val: _Optional[int] = ..., double_val: _Optional[float] = ..., float_val: _Optional[float] = ..., bool_val: bool = ..., bytes_list_val: _Optional[_Union[BytesList, _Mapping]] = ..., string_list_val: _Optional[_Union[StringList, _Mapping]] = ..., int32_list_val: _Optional[_Union[Int32List, _Mapping]] = ..., int64_list_val: _Optional[_Union[Int64List, _Mapping]] = ..., double_list_val: _Optional[_Union[DoubleList, _Mapping]] = ..., float_list_val: _Optional[_Union[FloatList, _Mapping]] = ..., bool_list_val: _Optional[_Union[BoolList, _Mapping]] = ...) -> None: ...

class ValueType(_message.Message):
    __slots__ = []
    class Enum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    BOOL: ValueType.Enum
    BOOL_LIST: ValueType.Enum
    BYTES: ValueType.Enum
    BYTES_LIST: ValueType.Enum
    DOUBLE: ValueType.Enum
    DOUBLE_LIST: ValueType.Enum
    FLOAT: ValueType.Enum
    FLOAT_LIST: ValueType.Enum
    INT32: ValueType.Enum
    INT32_LIST: ValueType.Enum
    INT64: ValueType.Enum
    INT64_LIST: ValueType.Enum
    INVALID: ValueType.Enum
    STRING: ValueType.Enum
    STRING_LIST: ValueType.Enum
    def __init__(self) -> None: ...
