from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FileFormat(_message.Message):
    __slots__ = ["parquet_format"]
    class ParquetFormat(_message.Message):
        __slots__ = []
        def __init__(self) -> None: ...
    PARQUET_FORMAT_FIELD_NUMBER: _ClassVar[int]
    parquet_format: FileFormat.ParquetFormat
    def __init__(self, parquet_format: _Optional[_Union[FileFormat.ParquetFormat, _Mapping]] = ...) -> None: ...

class StreamFormat(_message.Message):
    __slots__ = ["avro_format", "proto_format"]
    class AvroFormat(_message.Message):
        __slots__ = ["schema_json"]
        SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
        schema_json: str
        def __init__(self, schema_json: _Optional[str] = ...) -> None: ...
    class ProtoFormat(_message.Message):
        __slots__ = ["class_path"]
        CLASS_PATH_FIELD_NUMBER: _ClassVar[int]
        class_path: str
        def __init__(self, class_path: _Optional[str] = ...) -> None: ...
    AVRO_FORMAT_FIELD_NUMBER: _ClassVar[int]
    PROTO_FORMAT_FIELD_NUMBER: _ClassVar[int]
    avro_format: StreamFormat.AvroFormat
    proto_format: StreamFormat.ProtoFormat
    def __init__(self, avro_format: _Optional[_Union[StreamFormat.AvroFormat, _Mapping]] = ..., proto_format: _Optional[_Union[StreamFormat.ProtoFormat, _Mapping]] = ...) -> None: ...
