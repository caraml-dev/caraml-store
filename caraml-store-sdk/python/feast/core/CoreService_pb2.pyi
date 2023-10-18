from feast.core import Entity_pb2 as _Entity_pb2
from feast.core import Feature_pb2 as _Feature_pb2
from feast.core import FeatureTable_pb2 as _FeatureTable_pb2
from feast.core import OnlineStore_pb2 as _OnlineStore_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApplyEntityRequest(_message.Message):
    __slots__ = ["project", "spec"]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    SPEC_FIELD_NUMBER: _ClassVar[int]
    project: str
    spec: _Entity_pb2.EntitySpec
    def __init__(self, spec: _Optional[_Union[_Entity_pb2.EntitySpec, _Mapping]] = ..., project: _Optional[str] = ...) -> None: ...

class ApplyEntityResponse(_message.Message):
    __slots__ = ["entity"]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    entity: _Entity_pb2.Entity
    def __init__(self, entity: _Optional[_Union[_Entity_pb2.Entity, _Mapping]] = ...) -> None: ...

class ApplyFeatureTableRequest(_message.Message):
    __slots__ = ["project", "table_spec"]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    TABLE_SPEC_FIELD_NUMBER: _ClassVar[int]
    project: str
    table_spec: _FeatureTable_pb2.FeatureTableSpec
    def __init__(self, project: _Optional[str] = ..., table_spec: _Optional[_Union[_FeatureTable_pb2.FeatureTableSpec, _Mapping]] = ...) -> None: ...

class ApplyFeatureTableResponse(_message.Message):
    __slots__ = ["table"]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: _FeatureTable_pb2.FeatureTable
    def __init__(self, table: _Optional[_Union[_FeatureTable_pb2.FeatureTable, _Mapping]] = ...) -> None: ...

class ArchiveOnlineStoreRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ArchiveOnlineStoreResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ArchiveProjectRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ArchiveProjectResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class CreateProjectRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class CreateProjectResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class DeleteFeatureTableRequest(_message.Message):
    __slots__ = ["name", "project"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    name: str
    project: str
    def __init__(self, project: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class DeleteFeatureTableResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetEntityRequest(_message.Message):
    __slots__ = ["name", "project"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    name: str
    project: str
    def __init__(self, name: _Optional[str] = ..., project: _Optional[str] = ...) -> None: ...

class GetEntityResponse(_message.Message):
    __slots__ = ["entity"]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    entity: _Entity_pb2.Entity
    def __init__(self, entity: _Optional[_Union[_Entity_pb2.Entity, _Mapping]] = ...) -> None: ...

class GetFeastCoreVersionRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetFeastCoreVersionResponse(_message.Message):
    __slots__ = ["version"]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: str
    def __init__(self, version: _Optional[str] = ...) -> None: ...

class GetFeatureTableRequest(_message.Message):
    __slots__ = ["name", "project"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    name: str
    project: str
    def __init__(self, project: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class GetFeatureTableResponse(_message.Message):
    __slots__ = ["table"]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: _FeatureTable_pb2.FeatureTable
    def __init__(self, table: _Optional[_Union[_FeatureTable_pb2.FeatureTable, _Mapping]] = ...) -> None: ...

class GetOnlineStoreRequest(_message.Message):
    __slots__ = ["name"]
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class GetOnlineStoreResponse(_message.Message):
    __slots__ = ["online_store", "status"]
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ACTIVE: GetOnlineStoreResponse.Status
    ARCHIVED: GetOnlineStoreResponse.Status
    ONLINE_STORE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    online_store: _OnlineStore_pb2.OnlineStore
    status: GetOnlineStoreResponse.Status
    def __init__(self, online_store: _Optional[_Union[_OnlineStore_pb2.OnlineStore, _Mapping]] = ..., status: _Optional[_Union[GetOnlineStoreResponse.Status, str]] = ...) -> None: ...

class ListEntitiesRequest(_message.Message):
    __slots__ = ["filter"]
    class Filter(_message.Message):
        __slots__ = ["labels", "project"]
        class LabelsEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        LABELS_FIELD_NUMBER: _ClassVar[int]
        PROJECT_FIELD_NUMBER: _ClassVar[int]
        labels: _containers.ScalarMap[str, str]
        project: str
        def __init__(self, project: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: ListEntitiesRequest.Filter
    def __init__(self, filter: _Optional[_Union[ListEntitiesRequest.Filter, _Mapping]] = ...) -> None: ...

class ListEntitiesResponse(_message.Message):
    __slots__ = ["entities"]
    ENTITIES_FIELD_NUMBER: _ClassVar[int]
    entities: _containers.RepeatedCompositeFieldContainer[_Entity_pb2.Entity]
    def __init__(self, entities: _Optional[_Iterable[_Union[_Entity_pb2.Entity, _Mapping]]] = ...) -> None: ...

class ListFeatureTablesRequest(_message.Message):
    __slots__ = ["filter"]
    class Filter(_message.Message):
        __slots__ = ["labels", "project"]
        class LabelsEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        LABELS_FIELD_NUMBER: _ClassVar[int]
        PROJECT_FIELD_NUMBER: _ClassVar[int]
        labels: _containers.ScalarMap[str, str]
        project: str
        def __init__(self, project: _Optional[str] = ..., labels: _Optional[_Mapping[str, str]] = ...) -> None: ...
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: ListFeatureTablesRequest.Filter
    def __init__(self, filter: _Optional[_Union[ListFeatureTablesRequest.Filter, _Mapping]] = ...) -> None: ...

class ListFeatureTablesResponse(_message.Message):
    __slots__ = ["tables"]
    TABLES_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[_FeatureTable_pb2.FeatureTable]
    def __init__(self, tables: _Optional[_Iterable[_Union[_FeatureTable_pb2.FeatureTable, _Mapping]]] = ...) -> None: ...

class ListFeaturesRequest(_message.Message):
    __slots__ = ["filter"]
    class Filter(_message.Message):
        __slots__ = ["entities", "labels", "project"]
        class LabelsEntry(_message.Message):
            __slots__ = ["key", "value"]
            KEY_FIELD_NUMBER: _ClassVar[int]
            VALUE_FIELD_NUMBER: _ClassVar[int]
            key: str
            value: str
            def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
        ENTITIES_FIELD_NUMBER: _ClassVar[int]
        LABELS_FIELD_NUMBER: _ClassVar[int]
        PROJECT_FIELD_NUMBER: _ClassVar[int]
        entities: _containers.RepeatedScalarFieldContainer[str]
        labels: _containers.ScalarMap[str, str]
        project: str
        def __init__(self, labels: _Optional[_Mapping[str, str]] = ..., entities: _Optional[_Iterable[str]] = ..., project: _Optional[str] = ...) -> None: ...
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: ListFeaturesRequest.Filter
    def __init__(self, filter: _Optional[_Union[ListFeaturesRequest.Filter, _Mapping]] = ...) -> None: ...

class ListFeaturesResponse(_message.Message):
    __slots__ = ["features"]
    class FeaturesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _Feature_pb2.FeatureSpec
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_Feature_pb2.FeatureSpec, _Mapping]] = ...) -> None: ...
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    features: _containers.MessageMap[str, _Feature_pb2.FeatureSpec]
    def __init__(self, features: _Optional[_Mapping[str, _Feature_pb2.FeatureSpec]] = ...) -> None: ...

class ListOnlineStoresRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListOnlineStoresResponse(_message.Message):
    __slots__ = ["online_store"]
    ONLINE_STORE_FIELD_NUMBER: _ClassVar[int]
    online_store: _containers.RepeatedCompositeFieldContainer[_OnlineStore_pb2.OnlineStore]
    def __init__(self, online_store: _Optional[_Iterable[_Union[_OnlineStore_pb2.OnlineStore, _Mapping]]] = ...) -> None: ...

class ListProjectsRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class ListProjectsResponse(_message.Message):
    __slots__ = ["projects"]
    PROJECTS_FIELD_NUMBER: _ClassVar[int]
    projects: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, projects: _Optional[_Iterable[str]] = ...) -> None: ...

class RegisterOnlineStoreRequest(_message.Message):
    __slots__ = ["online_store"]
    ONLINE_STORE_FIELD_NUMBER: _ClassVar[int]
    online_store: _OnlineStore_pb2.OnlineStore
    def __init__(self, online_store: _Optional[_Union[_OnlineStore_pb2.OnlineStore, _Mapping]] = ...) -> None: ...

class RegisterOnlineStoreResponse(_message.Message):
    __slots__ = ["online_store", "status"]
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    NO_CHANGE: RegisterOnlineStoreResponse.Status
    ONLINE_STORE_FIELD_NUMBER: _ClassVar[int]
    REGISTERED: RegisterOnlineStoreResponse.Status
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UPDATED: RegisterOnlineStoreResponse.Status
    online_store: _OnlineStore_pb2.OnlineStore
    status: RegisterOnlineStoreResponse.Status
    def __init__(self, online_store: _Optional[_Union[_OnlineStore_pb2.OnlineStore, _Mapping]] = ..., status: _Optional[_Union[RegisterOnlineStoreResponse.Status, str]] = ...) -> None: ...
