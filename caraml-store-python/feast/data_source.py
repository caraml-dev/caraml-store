import enum
from typing import Dict, Optional

from feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.data_format import FileFormat, StreamFormat


class SourceType(enum.Enum):
    """
    DataSource value type. Used to define source types in DataSource.
    """

    UNKNOWN = 0
    BATCH_FILE = 1
    BATCH_BIGQUERY = 2


class FileOptions:
    """
    DataSource File options used to source features from a file
    """

    def __init__(
        self,
        file_format: FileFormat,
        file_url: str,
    ):
        self._file_format = file_format
        self._file_url = file_url

    @property
    def file_format(self):
        """
        Returns the file format of this file
        """
        return self._file_format

    @file_format.setter
    def file_format(self, file_format):
        """
        Sets the file format of this file
        """
        self._file_format = file_format

    @property
    def file_url(self):
        """
        Returns the file url of this file
        """
        return self._file_url

    @file_url.setter
    def file_url(self, file_url):
        """
        Sets the file url of this file
        """
        self._file_url = file_url

    @classmethod
    def from_proto(cls, file_options_proto: DataSourceProto.FileOptions):
        """
        Creates a FileOptions from a protobuf representation of a file option
        args:
            file_options_proto: a protobuf representation of a datasource
        Returns:
            Returns a FileOptions object based on the file_options protobuf
        """
        file_options = cls(
            file_format=FileFormat.from_proto(file_options_proto.file_format),
            file_url=file_options_proto.file_url,
        )
        return file_options

    def to_proto(self) -> DataSourceProto.FileOptions:
        """
        Converts an FileOptionsProto object to its protobuf representation.
        Returns:
            FileOptionsProto protobuf
        """

        file_options_proto = DataSourceProto.FileOptions(
            file_format=self.file_format.to_proto(),
            file_url=self.file_url,
        )

        return file_options_proto


class BigQueryOptions:
    """
    DataSource BigQuery options used to source features from BigQuery query
    """

    def __init__(
        self,
        table_ref: str,
    ):
        self._table_ref = table_ref

    @property
    def table_ref(self):
        """
        Returns the table ref of this BQ table
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this BQ table
        """
        self._table_ref = table_ref

    @classmethod
    def from_proto(cls, bigquery_options_proto: DataSourceProto.BigQueryOptions):
        """
        Creates a BigQueryOptions from a protobuf representation of a BigQuery option
        Args:
            bigquery_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a BigQueryOptions object based on the bigquery_options protobuf
        """

        bigquery_options = cls(
            table_ref=bigquery_options_proto.table_ref,
        )

        return bigquery_options

    def to_proto(self) -> DataSourceProto.BigQueryOptions:
        """
        Converts an BigQueryOptionsProto object to its protobuf representation.
        Returns:
            BigQueryOptionsProto protobuf
        """

        bigquery_options_proto = DataSourceProto.BigQueryOptions(
            table_ref=self.table_ref,
        )

        return bigquery_options_proto


class DataSource:
    """
    DataSource that can be used source features
    """

    def __init__(
        self,
        event_timestamp_column: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        self._event_timestamp_column = event_timestamp_column
        self._created_timestamp_column = created_timestamp_column
        self._field_mapping = field_mapping if field_mapping else {}
        self._date_partition_column = date_partition_column

    def __eq__(self, other):
        if not isinstance(other, DataSource):
            raise TypeError("Comparisons should only involve DataSource class objects.")

        if (
            self.event_timestamp_column != other.event_timestamp_column
            or self.created_timestamp_column != other.created_timestamp_column
            or self.field_mapping != other.field_mapping
            or self.date_partition_column != other.date_partition_column
        ):
            return False

        return True

    @property
    def field_mapping(self):
        """
        Returns the field mapping of this data source
        """
        return self._field_mapping

    @field_mapping.setter
    def field_mapping(self, field_mapping):
        """
        Sets the field mapping of this data source
        """
        self._field_mapping = field_mapping

    @property
    def event_timestamp_column(self):
        """
        Returns the event timestamp column of this data source
        """
        return self._event_timestamp_column

    @event_timestamp_column.setter
    def event_timestamp_column(self, event_timestamp_column):
        """
        Sets the event timestamp column of this data source
        """
        self._event_timestamp_column = event_timestamp_column

    @property
    def created_timestamp_column(self):
        """
        Returns the created timestamp column of this data source
        """
        return self._created_timestamp_column

    @created_timestamp_column.setter
    def created_timestamp_column(self, created_timestamp_column):
        """
        Sets the created timestamp column of this data source
        """
        self._created_timestamp_column = created_timestamp_column

    @property
    def date_partition_column(self):
        """
        Returns the date partition column of this data source
        """
        return self._date_partition_column

    @date_partition_column.setter
    def date_partition_column(self, date_partition_column):
        """
        Sets the date partition column of this data source
        """
        self._date_partition_column = date_partition_column

    @staticmethod
    def from_proto(data_source):
        """
        Convert data source config in FeatureTable spec to a DataSource class object.
        """

        if data_source.file_options.file_format and data_source.file_options.file_url:
            data_source_obj = FileSource(
                field_mapping=data_source.field_mapping,
                file_format=FileFormat.from_proto(data_source.file_options.file_format),
                file_url=data_source.file_options.file_url,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )
        elif data_source.bigquery_options.table_ref:
            data_source_obj = BigQuerySource(
                field_mapping=data_source.field_mapping,
                table_ref=data_source.bigquery_options.table_ref,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )
        elif (
            data_source.kafka_options.bootstrap_servers
            and data_source.kafka_options.topic
            and data_source.kafka_options.message_format
        ):
            data_source_obj = KafkaSource(
                field_mapping=data_source.field_mapping,
                bootstrap_servers=data_source.kafka_options.bootstrap_servers,
                message_format=StreamFormat.from_proto(
                    data_source.kafka_options.message_format
                ),
                topic=data_source.kafka_options.topic,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )
        else:
            raise ValueError("Could not identify the source type being added")

        return data_source_obj

    def to_proto(self) -> DataSourceProto:
        """
        Converts an DataSourceProto object to its protobuf representation.
        """
        raise NotImplementedError


class FileSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: str,
        file_format: FileFormat,
        file_url: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._file_options = FileOptions(file_format=file_format, file_url=file_url)

    def __eq__(self, other):
        if not isinstance(other, FileSource):
            raise TypeError("Comparisons should only involve FileSource class objects.")

        if (
            self.file_options.file_url != other.file_options.file_url
            or self.file_options.file_format != other.file_options.file_format
        ):
            return False
        return True

    @property
    def file_options(self):
        """
        Returns the file options of this data source
        """
        return self._file_options

    @file_options.setter
    def file_options(self, file_options):
        """
        Sets the file options of this data source
        """
        self._file_options = file_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_FILE,
            field_mapping=self.field_mapping,
            file_options=self.file_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto


class BigQuerySource(DataSource):
    def __init__(
        self,
        event_timestamp_column: str,
        table_ref: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._bigquery_options = BigQueryOptions(
            table_ref=table_ref,
        )

    def __eq__(self, other):
        if not isinstance(other, BigQuerySource):
            raise TypeError(
                "Comparisons should only involve BigQuerySource class objects."
            )

        if self.bigquery_options.table_ref != other.bigquery_options.table_ref:
            return False

        return True

    @property
    def bigquery_options(self):
        """
        Returns the bigquery options of this data source
        """
        return self._bigquery_options

    @bigquery_options.setter
    def bigquery_options(self, bigquery_options):
        """
        Sets the bigquery options of this data source
        """
        self._bigquery_options = bigquery_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=self.bigquery_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto


class KafkaOptions:
    """
    DataSource Kafka options used to source features from Kafka messages
    """

    def __init__(
        self,
        bootstrap_servers: str,
        message_format: StreamFormat,
        topic: str,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._message_format = message_format
        self._topic = topic

    @property
    def bootstrap_servers(self):
        """
        Returns a comma-separated list of Kafka bootstrap servers
        """
        return self._bootstrap_servers

    @bootstrap_servers.setter
    def bootstrap_servers(self, bootstrap_servers):
        """
        Sets a comma-separated list of Kafka bootstrap servers
        """
        self._bootstrap_servers = bootstrap_servers

    @property
    def message_format(self):
        """
        Returns the data format that is used to encode the feature data in Kafka messages
        """
        return self._message_format

    @message_format.setter
    def message_format(self, message_format):
        """
        Sets the data format that is used to encode the feature data in Kafka messages
        """
        self._message_format = message_format

    @property
    def topic(self):
        """
        Returns the Kafka topic to collect feature data from
        """
        return self._topic

    @topic.setter
    def topic(self, topic):
        """
        Sets the Kafka topic to collect feature data from
        """
        self._topic = topic

    @classmethod
    def from_proto(cls, kafka_options_proto: DataSourceProto.KafkaOptions):
        """
        Creates a KafkaOptions from a protobuf representation of a kafka option
        Args:
            kafka_options_proto: A protobuf representation of a DataSource
        Returns:
            Returns a BigQueryOptions object based on the kafka_options protobuf
        """

        kafka_options = cls(
            bootstrap_servers=kafka_options_proto.bootstrap_servers,
            message_format=StreamFormat.from_proto(kafka_options_proto.message_format),
            topic=kafka_options_proto.topic,
        )

        return kafka_options

    def to_proto(self) -> DataSourceProto.KafkaOptions:
        """
        Converts an KafkaOptionsProto object to its protobuf representation.
        Returns:
            KafkaOptionsProto protobuf
        """

        kafka_options_proto = DataSourceProto.KafkaOptions(
            bootstrap_servers=self.bootstrap_servers,
            message_format=self.message_format.to_proto(),
            topic=self.topic,
        )

        return kafka_options_proto


class KafkaSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: str,
        bootstrap_servers: str,
        message_format: StreamFormat,
        topic: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = dict(),
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._kafka_options = KafkaOptions(
            bootstrap_servers=bootstrap_servers,
            message_format=message_format,
            topic=topic,
        )

    def __eq__(self, other):
        if not isinstance(other, KafkaSource):
            raise TypeError(
                "Comparisons should only involve KafkaSource class objects."
            )

        if (
            self.kafka_options.bootstrap_servers
            != other.kafka_options.bootstrap_servers
            or self.kafka_options.message_format != other.kafka_options.message_format
            or self.kafka_options.topic != other.kafka_options.topic
        ):
            return False

        return True

    @property
    def kafka_options(self):
        """
        Returns the kafka options of this data source
        """
        return self._kafka_options

    @kafka_options.setter
    def kafka_options(self, kafka_options):
        """
        Sets the kafka options of this data source
        """
        self._kafka_options = kafka_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.STREAM_KAFKA,
            field_mapping=self.field_mapping,
            kafka_options=self.kafka_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto
