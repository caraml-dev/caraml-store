import enum

from feast_spark.api.JobService_pb2 import JobType as JobTypeProto


class JobType(enum.Enum):
    """
    Feature value type. Used to define data types in Feature Tables.
    """
    INVALID_JOB = 0
    STREAM_INGESTION_JOB = 1
    BATCH_INGESTION_JOB = 2
    RETRIEVAL_JOB = 3

    def to_proto(self) -> JobTypeProto:
        return JobTypeProto.Value(self.name)
