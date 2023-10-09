from feast.core import DataSource_pb2 as _DataSource_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

BATCH_INGESTION_JOB: JobType
DESCRIPTOR: _descriptor.FileDescriptor
INVALID_JOB: JobType
JOB_STATUS_DONE: JobStatus
JOB_STATUS_ERROR: JobStatus
JOB_STATUS_INVALID: JobStatus
JOB_STATUS_PENDING: JobStatus
JOB_STATUS_RUNNING: JobStatus
RETRIEVAL_JOB: JobType
STREAM_INGESTION_JOB: JobType

class CancelJobRequest(_message.Message):
    __slots__ = ["job_id"]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class CancelJobResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class GetHealthMetricsRequest(_message.Message):
    __slots__ = ["project", "table_names"]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAMES_FIELD_NUMBER: _ClassVar[int]
    project: str
    table_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, project: _Optional[str] = ..., table_names: _Optional[_Iterable[str]] = ...) -> None: ...

class GetHealthMetricsResponse(_message.Message):
    __slots__ = ["failed", "passed"]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    failed: _containers.RepeatedScalarFieldContainer[str]
    passed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, passed: _Optional[_Iterable[str]] = ..., failed: _Optional[_Iterable[str]] = ...) -> None: ...

class GetHistoricalFeaturesRequest(_message.Message):
    __slots__ = ["entity_source", "feature_refs", "output_format", "output_location", "project"]
    ENTITY_SOURCE_FIELD_NUMBER: _ClassVar[int]
    FEATURE_REFS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_LOCATION_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    entity_source: _DataSource_pb2.DataSource
    feature_refs: _containers.RepeatedScalarFieldContainer[str]
    output_format: str
    output_location: str
    project: str
    def __init__(self, feature_refs: _Optional[_Iterable[str]] = ..., entity_source: _Optional[_Union[_DataSource_pb2.DataSource, _Mapping]] = ..., project: _Optional[str] = ..., output_location: _Optional[str] = ..., output_format: _Optional[str] = ...) -> None: ...

class GetHistoricalFeaturesResponse(_message.Message):
    __slots__ = ["id", "job_start_time", "log_uri", "output_file_uri"]
    ID_FIELD_NUMBER: _ClassVar[int]
    JOB_START_TIME_FIELD_NUMBER: _ClassVar[int]
    LOG_URI_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FILE_URI_FIELD_NUMBER: _ClassVar[int]
    id: str
    job_start_time: _timestamp_pb2.Timestamp
    log_uri: str
    output_file_uri: str
    def __init__(self, id: _Optional[str] = ..., output_file_uri: _Optional[str] = ..., job_start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., log_uri: _Optional[str] = ...) -> None: ...

class GetJobRequest(_message.Message):
    __slots__ = ["job_id"]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class GetJobResponse(_message.Message):
    __slots__ = ["job"]
    JOB_FIELD_NUMBER: _ClassVar[int]
    job: Job
    def __init__(self, job: _Optional[_Union[Job, _Mapping]] = ...) -> None: ...

class Job(_message.Message):
    __slots__ = ["batch_ingestion", "error_message", "hash", "id", "log_uri", "retrieval", "start_time", "status", "stream_ingestion", "type"]
    class OfflineToOnlineMeta(_message.Message):
        __slots__ = ["table_name"]
        TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
        table_name: str
        def __init__(self, table_name: _Optional[str] = ...) -> None: ...
    class RetrievalJobMeta(_message.Message):
        __slots__ = ["output_location"]
        OUTPUT_LOCATION_FIELD_NUMBER: _ClassVar[int]
        output_location: str
        def __init__(self, output_location: _Optional[str] = ...) -> None: ...
    class StreamToOnlineMeta(_message.Message):
        __slots__ = ["table_name"]
        TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
        table_name: str
        def __init__(self, table_name: _Optional[str] = ...) -> None: ...
    BATCH_INGESTION_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    HASH_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOG_URI_FIELD_NUMBER: _ClassVar[int]
    RETRIEVAL_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STREAM_INGESTION_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    batch_ingestion: Job.OfflineToOnlineMeta
    error_message: str
    hash: str
    id: str
    log_uri: str
    retrieval: Job.RetrievalJobMeta
    start_time: _timestamp_pb2.Timestamp
    status: JobStatus
    stream_ingestion: Job.StreamToOnlineMeta
    type: JobType
    def __init__(self, id: _Optional[str] = ..., type: _Optional[_Union[JobType, str]] = ..., status: _Optional[_Union[JobStatus, str]] = ..., hash: _Optional[str] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., retrieval: _Optional[_Union[Job.RetrievalJobMeta, _Mapping]] = ..., batch_ingestion: _Optional[_Union[Job.OfflineToOnlineMeta, _Mapping]] = ..., stream_ingestion: _Optional[_Union[Job.StreamToOnlineMeta, _Mapping]] = ..., log_uri: _Optional[str] = ..., error_message: _Optional[str] = ...) -> None: ...

class ListJobsRequest(_message.Message):
    __slots__ = ["include_terminated", "project", "table_name"]
    INCLUDE_TERMINATED_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    include_terminated: bool
    project: str
    table_name: str
    def __init__(self, include_terminated: bool = ..., table_name: _Optional[str] = ..., project: _Optional[str] = ...) -> None: ...

class ListJobsResponse(_message.Message):
    __slots__ = ["jobs"]
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[Job]
    def __init__(self, jobs: _Optional[_Iterable[_Union[Job, _Mapping]]] = ...) -> None: ...

class ScheduleOfflineToOnlineIngestionJobRequest(_message.Message):
    __slots__ = ["cron_schedule", "ingestion_timespan", "project", "table_name"]
    CRON_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    INGESTION_TIMESPAN_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    cron_schedule: str
    ingestion_timespan: int
    project: str
    table_name: str
    def __init__(self, project: _Optional[str] = ..., table_name: _Optional[str] = ..., ingestion_timespan: _Optional[int] = ..., cron_schedule: _Optional[str] = ...) -> None: ...

class ScheduleOfflineToOnlineIngestionJobResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class StartOfflineToOnlineIngestionJobRequest(_message.Message):
    __slots__ = ["delta_ingestion", "end_date", "project", "start_date", "table_name"]
    DELTA_INGESTION_FIELD_NUMBER: _ClassVar[int]
    END_DATE_FIELD_NUMBER: _ClassVar[int]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    START_DATE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    delta_ingestion: bool
    end_date: _timestamp_pb2.Timestamp
    project: str
    start_date: _timestamp_pb2.Timestamp
    table_name: str
    def __init__(self, project: _Optional[str] = ..., table_name: _Optional[str] = ..., start_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_date: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., delta_ingestion: bool = ...) -> None: ...

class StartOfflineToOnlineIngestionJobResponse(_message.Message):
    __slots__ = ["id", "job_start_time", "log_uri", "table_name"]
    ID_FIELD_NUMBER: _ClassVar[int]
    JOB_START_TIME_FIELD_NUMBER: _ClassVar[int]
    LOG_URI_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    job_start_time: _timestamp_pb2.Timestamp
    log_uri: str
    table_name: str
    def __init__(self, id: _Optional[str] = ..., job_start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., table_name: _Optional[str] = ..., log_uri: _Optional[str] = ...) -> None: ...

class UnscheduleOfflineToOnlineIngestionJobRequest(_message.Message):
    __slots__ = ["project", "table_name"]
    PROJECT_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    project: str
    table_name: str
    def __init__(self, project: _Optional[str] = ..., table_name: _Optional[str] = ...) -> None: ...

class UnscheduleOfflineToOnlineIngestionJobResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class JobType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class JobStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
