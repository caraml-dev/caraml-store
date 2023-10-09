from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from feast.core import DataSource_pb2 as _DataSource_pb2
from feast.core import Feature_pb2 as _Feature_pb2
from feast.core import OnlineStore_pb2 as _OnlineStore_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FeatureTable(_message.Message):
    __slots__ = ["meta", "spec"]
    META_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    meta: FeatureTableMeta
    spec: FeatureTableSpec
    def __init__(self, spec: _Optional[_Union[FeatureTableSpec, _Mapping]] = ..., meta: _Optional[_Union[FeatureTableMeta, _Mapping]] = ...) -> None: ...

class FeatureTableMeta(_message.Message):
    __slots__ = ["created_timestamp", "hash", "last_updated_timestamp", "revision"]
    CREATED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    LAST_UPDATED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    created_timestamp: _timestamp_pb2.Timestamp
    hash: str
    last_updated_timestamp: _timestamp_pb2.Timestamp
    revision: int
    def __init__(self, created_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., last_updated_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., revision: _Optional[int] = ..., hash: _Optional[str] = ...) -> None: ...

class FeatureTableSpec(_message.Message):
    __slots__ = ["batch_source", "entities", "features", "labels", "max_age", "name", "online_store", "staleness_threshold", "stream_source"]
    class LabelsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    BATCH_SOURCE_FIELD_NUMBER: _ClassVar[int]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    MAX_AGE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ONLINE_STORE_FIELD_NUMBER: _ClassVar[int]
    STALENESS_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    STREAM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    batch_source: _DataSource_pb2.DataSource
    entities: _containers.RepeatedScalarFieldContainer[str]
    features: _containers.RepeatedCompositeFieldContainer[_Feature_pb2.FeatureSpec]
    labels: _containers.ScalarMap[str, str]
    max_age: _duration_pb2.Duration
    name: str
    online_store: _OnlineStore_pb2.OnlineStore
    staleness_threshold: _duration_pb2.Duration
    stream_source: _DataSource_pb2.DataSource
    def __init__(self, name: _Optional[str] = ..., entities: _Optional[_Iterable[str]] = ..., features: _Optional[_Iterable[_Union[_Feature_pb2.FeatureSpec, _Mapping]]] = ..., labels: _Optional[_Mapping[str, str]] = ..., max_age: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., batch_source: _Optional[_Union[_DataSource_pb2.DataSource, _Mapping]] = ..., stream_source: _Optional[_Union[_DataSource_pb2.DataSource, _Mapping]] = ..., staleness_threshold: _Optional[_Union[_duration_pb2.Duration, _Mapping]] = ..., online_store: _Optional[_Union[_OnlineStore_pb2.OnlineStore, _Mapping]] = ...) -> None: ...
