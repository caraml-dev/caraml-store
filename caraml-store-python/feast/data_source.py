import enum
from dataclasses import dataclass
from typing import Dict, Optional, Union

from feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.core.SparkOverride_pb2 import SparkOverride as SparkOverrideProto
from feast.data_format import FileFormat, StreamFormat


class SourceType(enum.Enum):
    """
    DataSource value type. Used to define source types in DataSource.
    """

    UNKNOWN = 0
    BATCH_FILE = 1
    BATCH_BIGQUERY = 2


@dataclass
class SparkOverride:
    """
    Overridable spark configuration for ingestion jobs.
    """

    driver_cpu: int = 0
    executor_cpu: int = 0
    driver_memory: str = ""
    executor_memory: str = ""

    @classmethod
    def from_proto(cls, spark_override_proto: SparkOverrideProto):
        """
        Create a new instance from the corresponding protobuf representation.
        Args:
            spark_override_proto (object):
        Returns:
            A new instance of the class.
        """
        return cls(
            driver_cpu=spark_override_proto.driver_cpu,
            executor_cpu=spark_override_proto.executor_cpu,
            driver_memory=spark_override_proto.driver_memory,
            executor_memory=spark_override_proto.executor_memory,
        )

    def to_proto(self) -> SparkOverrideProto:
        """
        Returns:
            Protobuf representation of the instance.
        """
        return SparkOverrideProto(
            driver_cpu=self.driver_cpu,
            executor_cpu=self.executor_cpu,
            driver_memory=self.driver_memory,
            executor_memory=self.executor_memory,
        )


@dataclass
class FileOptions:
    """
    DataSource File options used to source features from a file
    """
    file_format: FileFormat
    file_url: str
    spark_override: Optional[SparkOverride] = None

    @classmethod
    def from_proto(cls, file_options_proto: DataSourceProto.FileOptions):
        """
        Create a new instance from the corresponding protobuf representation.
        args:
            file_options_proto: a protobuf representation of a datasource
        Returns:
            A new instance of the class.
        """
        return cls(
            file_format=FileFormat.from_proto(file_options_proto.file_format),
            file_url=file_options_proto.file_url,
            spark_override=SparkOverride.from_proto(file_options_proto.spark_override)
        )

    def to_proto(self) -> DataSourceProto.FileOptions:
        """
        Returns:
            Protobuf representation of the instance.
        """

        return DataSourceProto.FileOptions(
            file_format=self.file_format.to_proto(),
            file_url=self.file_url,
            spark_override=self.spark_override.to_proto() if self.spark_override else None,
        )


@dataclass
class BigQueryOptions:
    """
    DataSource BigQuery options used to source features from BigQuery query
    """
    table_ref: str
    spark_override: Optional[SparkOverride] = None

    @classmethod
    def from_proto(cls, bigquery_options_proto: DataSourceProto.BigQueryOptions):
        """
        Creates a BigQueryOptions from a protobuf representation of a BigQuery option
        Args:
            bigquery_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a BigQueryOptions object based on the bigquery_options protobuf
        """

        return cls(
            table_ref=bigquery_options_proto.table_ref,
            spark_override=SparkOverride.from_proto(bigquery_options_proto.spark_override),
        )

    def to_proto(self) -> DataSourceProto.BigQueryOptions:
        """
        Converts an BigQueryOptionsProto object to its protobuf representation.
        Returns:
            BigQueryOptionsProto protobuf
        """

        return DataSourceProto.BigQueryOptions(
            table_ref=self.table_ref,
            spark_override=self.spark_override.to_proto() if self.spark_override else None,
        )


@dataclass
class FileSource:
    event_timestamp_column: str
    file_format: FileFormat
    file_url: str
    created_timestamp_column: str = ""
    field_mapping: Optional[Dict[str, str]] = None
    date_partition_column: str = ""
    spark_override: Optional[SparkOverride] = None

    def to_proto(self) -> DataSourceProto:
        return DataSourceProto(
            type=DataSourceProto.BATCH_FILE,
            field_mapping=self.field_mapping,
            file_options=FileOptions(file_format=self.file_format, file_url=self.file_url,
                                     spark_override=self.spark_override).to_proto() if self.spark_override else None,
            event_timestamp_column=self.event_timestamp_column,
            created_timestamp_column=self.created_timestamp_column,
            date_partition_column=self.date_partition_column,
        )


@dataclass
class BigQuerySource:
    event_timestamp_column: str
    table_ref: str
    created_timestamp_column: str = ""
    field_mapping: Optional[Dict[str, str]] = None
    date_partition_column: str = ""
    spark_override: Optional[SparkOverride] = None

    def to_proto(self) -> DataSourceProto:
        return DataSourceProto(
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=BigQueryOptions(self.table_ref, self.spark_override).to_proto(),
            event_timestamp_column=self.event_timestamp_column,
            created_timestamp_column=self.created_timestamp_column,
            date_partition_column=self.date_partition_column,
        )


@dataclass
class KafkaOptions:
    """
    DataSource Kafka options used to source features from Kafka messages
    """
    bootstrap_servers: str
    message_format: StreamFormat
    topic: str
    spark_override: Optional[SparkOverride] = None

    @classmethod
    def from_proto(cls, kafka_options_proto: DataSourceProto.KafkaOptions):
        """
        Creates a KafkaOptions from a protobuf representation of a kafka option
        Args:
            kafka_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a BigQueryOptions object based on the kafka_options protobuf
        """

        return cls(
            bootstrap_servers=kafka_options_proto.bootstrap_servers,
            message_format=StreamFormat.from_proto(kafka_options_proto.message_format),
            topic=kafka_options_proto.topic,
            spark_override=SparkOverride.from_proto(kafka_options_proto.spark_override),
        )

    def to_proto(self) -> DataSourceProto.KafkaOptions:
        """
        Converts an KafkaOptionsProto object to its protobuf representation.
        Returns:
            KafkaOptionsProto protobuf
        """

        return DataSourceProto.KafkaOptions(
            bootstrap_servers=self.bootstrap_servers,
            message_format=self.message_format.to_proto(),
            topic=self.topic,
            spark_override=self.spark_override.to_proto() if self.spark_override else None,
        )


@dataclass
class KafkaSource:
    event_timestamp_column: str
    bootstrap_servers: str
    message_format: StreamFormat
    topic: str
    created_timestamp_column: str = ""
    field_mapping: Optional[Dict[str, str]] = None
    date_partition_column: str = ""
    spark_override: Optional[SparkOverride] = None

    def to_proto(self) -> DataSourceProto:
        return DataSourceProto(
            type=DataSourceProto.STREAM_KAFKA,
            field_mapping=self.field_mapping,
            kafka_options=KafkaOptions(bootstrap_servers=self.bootstrap_servers,
                                       message_format=self.message_format, topic=self.topic, spark_override=self.spark_override).to_proto(),
            event_timestamp_column=self.event_timestamp_column,
            created_timestamp_column=self.created_timestamp_column,
            date_partition_column=self.date_partition_column,
        )


def new_batch_source_from_proto(data_source: DataSourceProto) -> Union[BigQuerySource, FileSource]:
    """
    Convert data source config in FeatureTable spec proto to one of the data source model.
    """

    if data_source.file_options.file_format and data_source.file_options.file_url:
        return FileSource(
            field_mapping=dict(data_source.field_mapping),
            file_format=FileFormat.from_proto(data_source.file_options.file_format),
            file_url=data_source.file_options.file_url,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            spark_override=SparkOverride.from_proto(data_source.file_options.spark_override),
        )
    elif data_source.bigquery_options.table_ref:
        return BigQuerySource(
            field_mapping=dict(data_source.field_mapping),
            table_ref=data_source.bigquery_options.table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            spark_override=SparkOverride.from_proto(data_source.bigquery_options.spark_override),
        )
    else:
        raise ValueError("Could not identify the source type being added")


def new_stream_source_from_proto(data_source: DataSourceProto) -> KafkaSource:
    """
    Convert data source config in FeatureTable spec proto to one of the data source model.
    """
    if data_source.kafka_options.topic:
        return KafkaSource(
            field_mapping=dict(data_source.field_mapping),
            bootstrap_servers=data_source.kafka_options.bootstrap_servers,
            message_format=StreamFormat.from_proto(
                data_source.kafka_options.message_format
            ),
            topic=data_source.kafka_options.topic,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            spark_override=SparkOverride.from_proto(data_source.kafka_options.spark_override),
        )
    else:
        raise ValueError("Could not identify the source type being added")
