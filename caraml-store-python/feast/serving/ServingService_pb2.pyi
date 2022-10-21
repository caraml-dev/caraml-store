"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import feast.types.Value_pb2
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import google.protobuf.timestamp_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _FieldStatus:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _FieldStatusEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_FieldStatus.ValueType], builtins.type):  # noqa: F821
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    INVALID: _FieldStatus.ValueType  # 0
    """Status is unset for this field."""
    PRESENT: _FieldStatus.ValueType  # 1
    """Field value is present for this field and age is within max age."""
    NULL_VALUE: _FieldStatus.ValueType  # 2
    """Values could be found for entity key and age is within max age, but
    this field value is assigned a value on ingestion into feast.
    """
    NOT_FOUND: _FieldStatus.ValueType  # 3
    """Entity key did not return any values as they do not exist in Feast.
    This could suggest that the feature values have not yet been ingested
    into feast or the ingestion failed.
    """
    OUTSIDE_MAX_AGE: _FieldStatus.ValueType  # 4
    """Values could be found for entity key, but field values are outside the maximum
    allowable range.
    """
    INGESTION_FAILURE: _FieldStatus.ValueType  # 5
    """Values could be found for entity key, but are null and is due to ingestion failures"""

class FieldStatus(_FieldStatus, metaclass=_FieldStatusEnumTypeWrapper): ...

INVALID: FieldStatus.ValueType  # 0
"""Status is unset for this field."""
PRESENT: FieldStatus.ValueType  # 1
"""Field value is present for this field and age is within max age."""
NULL_VALUE: FieldStatus.ValueType  # 2
"""Values could be found for entity key and age is within max age, but
this field value is assigned a value on ingestion into feast.
"""
NOT_FOUND: FieldStatus.ValueType  # 3
"""Entity key did not return any values as they do not exist in Feast.
This could suggest that the feature values have not yet been ingested
into feast or the ingestion failed.
"""
OUTSIDE_MAX_AGE: FieldStatus.ValueType  # 4
"""Values could be found for entity key, but field values are outside the maximum
allowable range.
"""
INGESTION_FAILURE: FieldStatus.ValueType  # 5
"""Values could be found for entity key, but are null and is due to ingestion failures"""
global___FieldStatus = FieldStatus

class GetFeastServingInfoRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___GetFeastServingInfoRequest = GetFeastServingInfoRequest

class GetFeastServingInfoResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    VERSION_FIELD_NUMBER: builtins.int
    version: builtins.str
    """Feast version of this serving deployment."""
    def __init__(
        self,
        *,
        version: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["version", b"version"]) -> None: ...

global___GetFeastServingInfoResponse = GetFeastServingInfoResponse

class FeatureReference(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FEATURE_TABLE_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    feature_table: builtins.str
    """Name of the Feature Table to retrieve the feature from."""
    name: builtins.str
    """Name of the Feature to retrieve the feature from."""
    def __init__(
        self,
        *,
        feature_table: builtins.str = ...,
        name: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["feature_table", b"feature_table", "name", b"name"]) -> None: ...

global___FeatureReference = FeatureReference

class GetOnlineFeaturesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class EntityRow(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        class FieldsEntry(google.protobuf.message.Message):
            DESCRIPTOR: google.protobuf.descriptor.Descriptor

            KEY_FIELD_NUMBER: builtins.int
            VALUE_FIELD_NUMBER: builtins.int
            key: builtins.str
            @property
            def value(self) -> feast.types.Value_pb2.Value: ...
            def __init__(
                self,
                *,
                key: builtins.str = ...,
                value: feast.types.Value_pb2.Value | None = ...,
            ) -> None: ...
            def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
            def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

        TIMESTAMP_FIELD_NUMBER: builtins.int
        FIELDS_FIELD_NUMBER: builtins.int
        @property
        def timestamp(self) -> google.protobuf.timestamp_pb2.Timestamp:
            """Request timestamp of this row. This value will be used,
            together with maxAge, to determine feature staleness.
            """
        @property
        def fields(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, feast.types.Value_pb2.Value]:
            """Map containing mapping of entity name to entity value."""
        def __init__(
            self,
            *,
            timestamp: google.protobuf.timestamp_pb2.Timestamp | None = ...,
            fields: collections.abc.Mapping[builtins.str, feast.types.Value_pb2.Value] | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["timestamp", b"timestamp"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["fields", b"fields", "timestamp", b"timestamp"]) -> None: ...

    FEATURES_FIELD_NUMBER: builtins.int
    ENTITY_ROWS_FIELD_NUMBER: builtins.int
    PROJECT_FIELD_NUMBER: builtins.int
    @property
    def features(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___FeatureReference]:
        """List of features that are being retrieved"""
    @property
    def entity_rows(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___GetOnlineFeaturesRequest.EntityRow]:
        """List of entity rows, containing entity id and timestamp data.
        Used during retrieval of feature rows and for joining feature
        rows into a final dataset
        """
    project: builtins.str
    """Optional field to specify project name override. If specified, uses the
    given project for retrieval. Overrides the projects specified in
    Feature References if both are specified.
    """
    def __init__(
        self,
        *,
        features: collections.abc.Iterable[global___FeatureReference] | None = ...,
        entity_rows: collections.abc.Iterable[global___GetOnlineFeaturesRequest.EntityRow] | None = ...,
        project: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entity_rows", b"entity_rows", "features", b"features", "project", b"project"]) -> None: ...

global___GetOnlineFeaturesRequest = GetOnlineFeaturesRequest

class GetOnlineFeaturesResponseV2(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class FieldValues(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        class FieldsEntry(google.protobuf.message.Message):
            DESCRIPTOR: google.protobuf.descriptor.Descriptor

            KEY_FIELD_NUMBER: builtins.int
            VALUE_FIELD_NUMBER: builtins.int
            key: builtins.str
            @property
            def value(self) -> feast.types.Value_pb2.Value: ...
            def __init__(
                self,
                *,
                key: builtins.str = ...,
                value: feast.types.Value_pb2.Value | None = ...,
            ) -> None: ...
            def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
            def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

        class StatusesEntry(google.protobuf.message.Message):
            DESCRIPTOR: google.protobuf.descriptor.Descriptor

            KEY_FIELD_NUMBER: builtins.int
            VALUE_FIELD_NUMBER: builtins.int
            key: builtins.str
            value: global___FieldStatus.ValueType
            def __init__(
                self,
                *,
                key: builtins.str = ...,
                value: global___FieldStatus.ValueType = ...,
            ) -> None: ...
            def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

        FIELDS_FIELD_NUMBER: builtins.int
        STATUSES_FIELD_NUMBER: builtins.int
        @property
        def fields(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, feast.types.Value_pb2.Value]:
            """Map of feature or entity name to feature/entity values.
            Timestamps are not returned in this response.
            """
        @property
        def statuses(self) -> google.protobuf.internal.containers.ScalarMap[builtins.str, global___FieldStatus.ValueType]:
            """Map of feature or entity name to feature/entity statuses/metadata."""
        def __init__(
            self,
            *,
            fields: collections.abc.Mapping[builtins.str, feast.types.Value_pb2.Value] | None = ...,
            statuses: collections.abc.Mapping[builtins.str, global___FieldStatus.ValueType] | None = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["fields", b"fields", "statuses", b"statuses"]) -> None: ...

    FIELD_VALUES_FIELD_NUMBER: builtins.int
    @property
    def field_values(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___GetOnlineFeaturesResponseV2.FieldValues]:
        """Feature values retrieved from feast."""
    def __init__(
        self,
        *,
        field_values: collections.abc.Iterable[global___GetOnlineFeaturesResponseV2.FieldValues] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["field_values", b"field_values"]) -> None: ...

global___GetOnlineFeaturesResponseV2 = GetOnlineFeaturesResponseV2

class GetOnlineFeaturesResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class FieldVector(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        VALUES_FIELD_NUMBER: builtins.int
        STATUSES_FIELD_NUMBER: builtins.int
        @property
        def values(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[feast.types.Value_pb2.Value]: ...
        @property
        def statuses(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[global___FieldStatus.ValueType]: ...
        def __init__(
            self,
            *,
            values: collections.abc.Iterable[feast.types.Value_pb2.Value] | None = ...,
            statuses: collections.abc.Iterable[global___FieldStatus.ValueType] | None = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["statuses", b"statuses", "values", b"values"]) -> None: ...

    METADATA_FIELD_NUMBER: builtins.int
    RESULTS_FIELD_NUMBER: builtins.int
    @property
    def metadata(self) -> global___GetOnlineFeaturesResponseMetadata: ...
    @property
    def results(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___GetOnlineFeaturesResponse.FieldVector]:
        """Length of "results" array should match length of requested features and entities.
        We also preserve the same order of features here as in metadata.field_names
        """
    def __init__(
        self,
        *,
        metadata: global___GetOnlineFeaturesResponseMetadata | None = ...,
        results: collections.abc.Iterable[global___GetOnlineFeaturesResponse.FieldVector] | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata", b"metadata", "results", b"results"]) -> None: ...

global___GetOnlineFeaturesResponse = GetOnlineFeaturesResponse

class GetOnlineFeaturesResponseMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FIELD_NAMES_FIELD_NUMBER: builtins.int
    @property
    def field_names(self) -> global___FieldList: ...
    def __init__(
        self,
        *,
        field_names: global___FieldList | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field_names", b"field_names"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field_names", b"field_names"]) -> None: ...

global___GetOnlineFeaturesResponseMetadata = GetOnlineFeaturesResponseMetadata

class FieldList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    VAL_FIELD_NUMBER: builtins.int
    @property
    def val(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        val: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["val", b"val"]) -> None: ...

global___FieldList = FieldList
