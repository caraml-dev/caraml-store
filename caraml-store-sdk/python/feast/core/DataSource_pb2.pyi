from feast.core import DataFormat_pb2 as _DataFormat_pb2
from feast.core import SparkOverride_pb2 as _SparkOverride_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataSource(_message.Message):
    __slots__ = ["bigquery_options", "created_timestamp_column", "date_partition_column", "event_timestamp_column", "field_mapping", "file_options", "kafka_options", "type"]
    class SourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class BigQueryOptions(_message.Message):
        __slots__ = ["spark_override", "table_ref"]
        SPARK_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
        TABLE_REF_FIELD_NUMBER: _ClassVar[int]
        spark_override: _SparkOverride_pb2.SparkOverride
        table_ref: str
        def __init__(self, table_ref: _Optional[str] = ..., spark_override: _Optional[_Union[_SparkOverride_pb2.SparkOverride, _Mapping]] = ...) -> None: ...
    class FieldMappingEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class FileOptions(_message.Message):
        __slots__ = ["file_format", "file_url", "spark_override"]
        FILE_FORMAT_FIELD_NUMBER: _ClassVar[int]
        FILE_URL_FIELD_NUMBER: _ClassVar[int]
        SPARK_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
        file_format: _DataFormat_pb2.FileFormat
        file_url: str
        spark_override: _SparkOverride_pb2.SparkOverride
        def __init__(self, file_format: _Optional[_Union[_DataFormat_pb2.FileFormat, _Mapping]] = ..., file_url: _Optional[str] = ..., spark_override: _Optional[_Union[_SparkOverride_pb2.SparkOverride, _Mapping]] = ...) -> None: ...
    class KafkaOptions(_message.Message):
        __slots__ = ["bootstrap_servers", "message_format", "spark_override", "topic"]
        BOOTSTRAP_SERVERS_FIELD_NUMBER: _ClassVar[int]
        MESSAGE_FORMAT_FIELD_NUMBER: _ClassVar[int]
        SPARK_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
        TOPIC_FIELD_NUMBER: _ClassVar[int]
        bootstrap_servers: str
        message_format: _DataFormat_pb2.StreamFormat
        spark_override: _SparkOverride_pb2.SparkOverride
        topic: str
        def __init__(self, bootstrap_servers: _Optional[str] = ..., topic: _Optional[str] = ..., message_format: _Optional[_Union[_DataFormat_pb2.StreamFormat, _Mapping]] = ..., spark_override: _Optional[_Union[_SparkOverride_pb2.SparkOverride, _Mapping]] = ...) -> None: ...
    BATCH_BIGQUERY: DataSource.SourceType
    BATCH_FILE: DataSource.SourceType
    BIGQUERY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    CREATED_TIMESTAMP_COLUMN_FIELD_NUMBER: _ClassVar[int]
    DATE_PARTITION_COLUMN_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIMESTAMP_COLUMN_FIELD_NUMBER: _ClassVar[int]
    FIELD_MAPPING_FIELD_NUMBER: _ClassVar[int]
    FILE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    INVALID: DataSource.SourceType
    KAFKA_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    STREAM_KAFKA: DataSource.SourceType
    TYPE_FIELD_NUMBER: _ClassVar[int]
    bigquery_options: DataSource.BigQueryOptions
    created_timestamp_column: str
    date_partition_column: str
    event_timestamp_column: str
    field_mapping: _containers.ScalarMap[str, str]
    file_options: DataSource.FileOptions
    kafka_options: DataSource.KafkaOptions
    type: DataSource.SourceType
    def __init__(self, type: _Optional[_Union[DataSource.SourceType, str]] = ..., field_mapping: _Optional[_Mapping[str, str]] = ..., event_timestamp_column: _Optional[str] = ..., date_partition_column: _Optional[str] = ..., created_timestamp_column: _Optional[str] = ..., file_options: _Optional[_Union[DataSource.FileOptions, _Mapping]] = ..., bigquery_options: _Optional[_Union[DataSource.BigQueryOptions, _Mapping]] = ..., kafka_options: _Optional[_Union[DataSource.KafkaOptions, _Mapping]] = ...) -> None: ...
