import abc
import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import timedelta, datetime
from logging.config import dictConfig
from typing import Any, Dict, List, NamedTuple, Optional, cast

from jinja2 import FileSystemLoader, Environment
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as func
from pyspark.sql.functions import (
    broadcast,
    col,
    expr,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import LongType

EVENT_TIMESTAMP_ALIAS = "event_timestamp"
ENTITY_EVENT_TIMESTAMP_ALIAS = "event_timestamp_entity"
CREATED_TIMESTAMP_ALIAS = "created_timestamp"


def get_termination_log_path():
    if os.access("/dev/termination-log", os.W_OK):
        return "/dev/termination-log"
    return "/dev/stderr"


DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"standard": {"format": "%(asctime)s [%(levelname)s] %(message)s"}},
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "ERROR",
            "formatter": "standard",
            "filename": get_termination_log_path(),
            "mode": "a",
        },
    },
    "loggers": {"__main__": {"level": "INFO", "handlers": ["default", "file"]}},
}

dictConfig(DEFAULT_LOGGING)
logger = logging.getLogger(__name__)


class Source(abc.ABC):
    """
    Source for an entity or feature dataframe.

    Attributes:
        event_timestamp_column (str): Column representing the event timestamp.
        created_timestamp_column (str): Column representing the creation timestamp. Required
            only if the source corresponds to a feature table.
        field_mapping (Optional[Dict[str, str]]): If present, the source column will be renamed
            based on the mapping.
    """

    def __init__(
        self,
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        field_mapping: Optional[Dict[str, str]] = None,
    ):

        self.event_timestamp_column = event_timestamp_column
        self.created_timestamp_column = created_timestamp_column if created_timestamp_column else event_timestamp_column
        self.field_mapping = field_mapping if field_mapping else {}

    @property
    def spark_read_options(self) -> Dict[str, str]:
        """
        Return a dictionary of options which will be passed to spark when reading the
        data source. Refer to
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#manually-specifying-options
        for possible options.
        """
        return {}

    @property
    @abc.abstractmethod
    def spark_format(self) -> str:
        """
        Return the format corresponding to the datasource. Must be a format that is recognizable by
        the spark cluster which the historical feature retrieval job will run on.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def spark_path(self) -> str:
        """
        Return an uri that points to the datasource. The uri scheme must be recognizable by
        the spark cluster which the historical feature retrieval job will run on.
        """
        raise NotImplementedError


class FileSource(Source):
    """
    A file source which could either be located on the local file system or a remote directory .

    Attributes:
        format (str): File format.
        path (str): Uri to the file.
        event_timestamp_column (str): Column representing the event timestamp.
        created_timestamp_column (str): Column representing the creation timestamp. Required
            only if the source corresponds to a feature table.
        field_mapping (Dict[str, str]): Optional. If present, the source column will be renamed
            based on the mapping. The key would be the final result and the value would be the source column.
        options (Optional[Dict[str, str]]): Options to be passed to spark while reading the file source.
    """

    PROTO_FORMAT_TO_SPARK = {
        "ParquetFormat": "parquet",
        "AvroFormat": "avro",
        "CSVFormat": "csv",
    }

    def __init__(
        self,
        format: str,
        path: str,
        event_timestamp_column: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        super().__init__(
            event_timestamp_column, created_timestamp_column, field_mapping
        )
        self.format = format
        self.path = path
        self.options = options if options else {}

    @property
    def spark_format(self) -> str:
        return self.format

    @property
    def spark_path(self) -> str:
        return self.path

    @property
    def spark_read_options(self) -> Dict[str, str]:
        return self.options


class BigQuerySource(Source):
    """
    Big query datasource, which depends on spark bigquery connector (https://github.com/GoogleCloudDataproc/spark-bigquery-connector).

    Attributes:
        project (str): GCP project id.
        dataset (str): BQ dataset.
        table (str): BQ table.
        field_mapping (Dict[str, str]): Optional. If present, the source column will be renamed
            based on the mapping. The key would be the final result and value would be the source column.
        event_timestamp_column (str): Column representing the event timestamp.
        created_timestamp_column (str): Column representing the creation timestamp. Required
            only if the source corresponds to a feature table.
        materialization (Dict[str, str]): Optional. Destination for materialized view,
            e.g. dict(project="...", dataset="...).
    """

    def __init__(
        self,
        project: str,
        dataset: str,
        table: str,
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        field_mapping: Optional[Dict[str, str]],
        materialization: Optional[Dict[str, str]] = None,
    ):
        super().__init__(
            event_timestamp_column, created_timestamp_column, field_mapping
        )
        self.project = project
        self.dataset = dataset
        self.table = table
        self.materialization = materialization

    @property
    def spark_format(self) -> str:
        return "bigquery"

    @property
    def spark_path(self) -> str:
        return f"{self.project}:{self.dataset}.{self.table}"

    @property
    def bq_ref(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def spark_read_options(self) -> Dict[str, str]:
        opts = {**super().spark_read_options, "viewsEnabled": "true"}
        if self.materialization:
            opts.update(
                {
                    "materializationProject": self.materialization["project"],
                    "materializationDataset": self.materialization["dataset"],
                }
            )
        return opts


def source_from_dict(dct: Dict) -> Source:
    if "file" in dct.keys():
        return FileSource(
            format=FileSource.PROTO_FORMAT_TO_SPARK[
                dct["file"]["format"]["jsonClass"]
            ],
            path=dct["file"]["path"],
            event_timestamp_column=dct["file"]["eventTimestampColumn"],
            created_timestamp_column=dct["file"].get("createdTimestampColumn"),
            field_mapping=dct["file"].get("fieldMapping"),
            options=dct["file"].get("options"),
        )
    else:
        return BigQuerySource(
            project=dct["bq"]["project"],
            dataset=dct["bq"]["dataset"],
            table=dct["bq"]["table"],
            field_mapping=dct["bq"].get("fieldMapping", {}),
            event_timestamp_column=dct["bq"]["eventTimestampColumn"],
            created_timestamp_column=dct["bq"].get("createdTimestampColumn"),
            materialization=dct["bq"].get("materialization"),
        )


class Field(NamedTuple):
    """
    Defines name and type for Feast entities and features.

    Attributes:
        name (str): Field name.
        type (str): Feast type name.
    """

    name: str
    type: str

    @property
    def spark_type(self):
        """
        Returns Spark data type that corresponds to the field's Feast type
        """
        feast_to_spark_type_mapping = {
            "bytes": "binary",
            "string": "string",
            "int32": "int",
            "int64": "bigint",
            "double": "double",
            "float": "float",
            "bool": "boolean",
            "bytes_list": "array<binary>",
            "string_list": "array<string>",
            "int32_list": "array<int>",
            "int64_list": "array<bigint>",
            "double_list": "array<double>",
            "float_list": "array<float>",
            "bool_list": "array<boolean>",
        }
        return feast_to_spark_type_mapping[self.type.lower()]


class FeatureTable(NamedTuple):
    """
    Feature table specification.

    Attributes:
        name (str): Table name.
        entities (List[Field]): Primary keys for the features.
        features (List[Field]): Feature list.
        max_age (int): In seconds. determines the lower bound of the timestamp of the retrieved feature.
        project (str): Feast project name.
    """

    name: str
    entities: List[Field]
    features: List[Field]
    max_age: int
    project: Optional[str] = None

    @property
    def entity_names(self):
        """
        Returns columns names for the entities.
        """
        return [field.name for field in self.entities]

    @property
    def feature_names(self):
        """
        Returns columns names for the features.
        """
        return [field.name for field in self.features]


class FileDestination(NamedTuple):
    """
    Output file for the spark job.

    Attributes:
        format (str): Output format.
        path (str): Output uri.
    """

    format: str
    path: str


@dataclass
class BigQueryFeatureView:
    name: str
    entities: List[str]
    features: List[str]
    project: str
    bq_ref: str
    event_timestamp_column: str
    created_timestamp_column: Optional[str]
    field_mapping: Dict[str, str]
    max_age: int
    min_event_timestamp: datetime
    max_event_timestamp: datetime

    @classmethod
    def from_feature_table(cls, table: FeatureTable, source: BigQuerySource, entity_min_event_timestamp: datetime, entity_max_event_timestamp: datetime):
        return BigQueryFeatureView(
            name=table.name,
            entities=table.entity_names,
            features=table.feature_names,
            project=table.project,
            bq_ref=source.bq_ref,
            event_timestamp_column=source.event_timestamp_column,
            created_timestamp_column=source.event_timestamp_column,
            field_mapping=source.field_mapping,
            max_age=table.max_age,
            min_event_timestamp=entity_min_event_timestamp - timedelta(seconds=table.max_age),
            max_event_timestamp=entity_max_event_timestamp
        )

    @property
    def entity_selections(self):
        return [f"{self.field_mapping.get(entity, entity)} as {entity}" for entity in self.entities]


def _map_column(df: DataFrame, col_mapping: Dict[str, str]):
    source_to_alias_map = {v: k for k, v in col_mapping.items()}
    projection = {}

    for col_name in df.columns + list(set(col_mapping) - set(df.columns)):
        if col_name in source_to_alias_map:
            # column rename
            projection[source_to_alias_map.get(col_name)] = col(col_name)
        else:
            projection[col_name] = expr(col_mapping.get(col_name, col_name))

    return df.select([c.alias(str(a)) for a, c in projection.items()])


def as_of_join(
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
    feature_table_df: DataFrame,
    feature_table: FeatureTable,
) -> DataFrame:
    """Perform an as of join between entity and feature table, given a maximum age tolerance.
    Join conditions:
    1. Entity primary key(s) value matches.
    2. Feature event timestamp is the closest match possible to the entity event timestamp,
       but must not be more recent than the entity event timestamp, and the difference must
       not be greater than max_age, unless max_age is not specified.
    3. If more than one feature table rows satisfy condition 1 and 2, feature row with the
       most recent created timestamp will be chosen.
    4. If none of the above conditions are satisfied, the feature rows will have null values.

    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_df (Dataframe): Spark dataframe representing the feature table.
        feature_table (FeatureTable): Feature table specification, which provide information on
            how the join should be performed, such as the entity primary keys and max age.

    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_table, where the feature columns will
            be prefixed with feature table name.

    Example:
        >>> entity_df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> feature_table_1_df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_1.max_age
            None
        >>> feature_table_1.name
            'table1'
        >>> df = as_of_join(entity_df, "event_timestamp", feature_table_1_df, feature_table_1)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table1__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|            200|
            +------+-------------------+---------------+

        >>> feature_table_2.df.show()
            +------+-------+-------------------+-------------------+
            |entity|feature|    event_timestamp|  created_timestamp|
            +------+-------+-------------------+-------------------+
            |    10|    200|2020-09-01 00:00:00|2020-09-02 00:00:00|
            +------+-------+-------------------+-------------------+
            |    10|    400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+-------+-------------------+-------------------+
        >>> feature_table_2.max_age
            43200
        >>> feature_table_2.name
            'table2'
        >>> df = as_of_join(entity_df, "event_timestamp", feature_table_2_df, feature_table_2)
        >>> df.show()
            +------+-------------------+---------------+
            |entity|    event_timestamp|table2__feature|
            +------+-------------------+---------------+
            |  1001|2020-09-02 00:00:00|           null|
            +------+-------------------+---------------+

    """
    entity_with_id = entity_df.withColumn("_row_nr", monotonically_increasing_id())

    feature_event_timestamp_column_with_prefix = (
        f"{feature_table.name}__{EVENT_TIMESTAMP_ALIAS}"
    )
    feature_created_timestamp_column_with_prefix = (
        f"{feature_table.name}__{CREATED_TIMESTAMP_ALIAS}"
    )

    projection = [
        col(col_name).alias(f"{feature_table.name}__{col_name}")
        for col_name in feature_table_df.columns
    ]

    aliased_feature_table_df = feature_table_df.select(projection)

    join_cond = (
        entity_with_id[entity_event_timestamp_column]
        == aliased_feature_table_df[
            f"{feature_table.name}__{ENTITY_EVENT_TIMESTAMP_ALIAS}"
        ]
    )

    for key in feature_table.entity_names:
        join_cond = join_cond & (
            entity_with_id[key]
            == aliased_feature_table_df[f"{feature_table.name}__{key}"]
        )

    conditional_join = entity_with_id.join(
        aliased_feature_table_df, join_cond, "leftOuter"
    )
    for key in feature_table.entity_names:
        conditional_join = conditional_join.drop(
            aliased_feature_table_df[f"{feature_table.name}__{key}"]
        )

    window = Window.partitionBy("_row_nr", *feature_table.entity_names).orderBy(
        col(feature_event_timestamp_column_with_prefix).desc(),
        col(feature_created_timestamp_column_with_prefix).desc(),
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity_df.columns
        + [
            f"{feature_table.name}__{feature}"
            for feature in feature_table.feature_names
        ]
    )


def join_entity_to_feature_tables(
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
    feature_table_dfs: List[DataFrame],
    feature_tables: List[FeatureTable],
) -> DataFrame:
    """Perform as of join between entity and multiple feature table.

    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_dfs (List[Dataframe]): List of Spark dataframes representing the feature tables.
        feature_tables (List[FeatureTable]): List of feature table specification. The length and ordering
            of this argument must follow that of feature_table_dfs.

    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_tables, where the feature columns will
            be prefixed with feature table name.

    Example:
        >>> entity_df.show()
            +------+-------------------+
            |entity|    event_timestamp|
            +------+-------------------+
            |  1001|2020-09-02 00:00:00|
            +------+-------------------+

        >>> table1_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature1|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     200|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------

        >>> table1 = FeatureTable(
                name="table1",
                features=[Field("feature1", "int32")],
                entities=[Field("entity", "int32")],
            )

        >>> table2_df.show()
            +------+--------+-------------------+-------------------+
            |entity|feature2|    event_timestamp|  created_timestamp|
            +------+--------+-------------------+-------------------+
            |    10|     400|2020-09-01 00:00:00|2020-09-01 00:00:00|
            +------+--------+-------------------+-------------------

        >>> table2 = FeatureTable(
                name="table2",
                features=[Field("feature2", "int32")],
                entities=[Field("entity", "int32")],
            )

        >>> tables = [table1, table2]

        >>> joined_df = join_entity_to_feature_tables(
                entity,
                tables,
            )
        >>> joined_df = join_entity_to_feature_tables(entity_df, "event_timestamp",
                [table1_df, table2_df], [table1, table2])

        >>> joined_df.show()
            +------+-------------------+----------------+----------------+
            |entity|    event_timestamp|table1__feature1|table2__feature2|
            +------+-------------------+----------------+----------------+
            |  1001|2020-09-02 00:00:00|             200|             400|
            +------+-------------------+----------------+----------------+
    """
    joined_df = entity_df

    for (feature_table_df, feature_table,) in zip(feature_table_dfs, feature_tables):
        joined_df = as_of_join(
            joined_df, entity_event_timestamp_column, feature_table_df, feature_table,
        )
        if SparkContext._active_spark_context._jsc.sc().getCheckpointDir().nonEmpty():  # type: ignore
            joined_df = joined_df.checkpoint()

    return joined_df


class SchemaError(Exception):
    """
    One or more columns in entity or feature table dataframe are either missing
    or have the wrong data types
    """

    pass


def filter_feature_table_by_time_range(
    feature_table_df: DataFrame,
    feature_table: FeatureTable,
    feature_event_timestamp_column: str,
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
) -> DataFrame:
    entity_max_timestamp = entity_df.agg(
        {entity_event_timestamp_column: "max"}
    ).collect()[0][0]
    entity_min_timestamp = entity_df.agg(
        {entity_event_timestamp_column: "min"}
    ).collect()[0][0]

    feature_table_timestamp_filter = (
        col(feature_event_timestamp_column).between(
            entity_min_timestamp - timedelta(seconds=feature_table.max_age),
            entity_max_timestamp,
        )
        if feature_table.max_age
        else col(feature_event_timestamp_column) <= entity_max_timestamp
    )

    time_range_filtered_df = feature_table_df.filter(feature_table_timestamp_filter)

    entities_projected = (
        entity_df.withColumnRenamed(
            entity_event_timestamp_column, ENTITY_EVENT_TIMESTAMP_ALIAS
        )
        .select(feature_table.entity_names + [ENTITY_EVENT_TIMESTAMP_ALIAS])
        .distinct()
    )

    time_range_filtered_df = (
        time_range_filtered_df.repartition(200)
        .join(
            broadcast(entities_projected), on=feature_table.entity_names, how="inner",
        )
        .withColumn(
            "distance",
            col(ENTITY_EVENT_TIMESTAMP_ALIAS).cast("long")
            - col(EVENT_TIMESTAMP_ALIAS).cast("long"),
        )
        .where((col("distance") >= 0) & (col("distance") <= feature_table.max_age))
        .withColumn(
            "min_distance",
            func.min("distance").over(
                Window.partitionBy(
                    feature_table.entity_names + [ENTITY_EVENT_TIMESTAMP_ALIAS]
                )
            ),
        )
        .where(col("distance") == col("min_distance"))
    )
    if SparkContext._active_spark_context._jsc.sc().getCheckpointDir().nonEmpty():  # type: ignore
        time_range_filtered_df = time_range_filtered_df.checkpoint()

    return time_range_filtered_df


def _read_and_verify_entity_df_from_source(
    spark: SparkSession, source: Source
) -> DataFrame:
    entity_df = (
        spark.read.format(source.spark_format)
        .options(**source.spark_read_options)
        .load(source.spark_path)
    )

    mapped_entity_df = _map_column(entity_df, source.field_mapping)

    if source.event_timestamp_column not in mapped_entity_df.columns:
        raise SchemaError(
            f"{source.event_timestamp_column} is missing for the entity dataframe."
        )

    return mapped_entity_df


def _type_casting_allowed(feature_type: str, source_col_type):
    allowed_casting_for_source_col = {"double": ["float"]}

    if feature_type == source_col_type:
        return True

    allowed_feature_type_for_casting = allowed_casting_for_source_col.get(
        source_col_type
    )

    return (
        allowed_feature_type_for_casting is not None
        and feature_type in allowed_feature_type_for_casting
    )


def _read_and_verify_feature_table_df_from_source(
    spark: SparkSession, feature_table: FeatureTable, source: Source,
) -> DataFrame:
    source_df = (
        spark.read.format(source.spark_format)
        .options(**source.spark_read_options)
        .load(source.spark_path)
    )

    mapped_source_df = _map_column(source_df, source.field_mapping)

    if not source.created_timestamp_column:
        raise SchemaError(
            "Created timestamp column must not be none for feature table."
        )

    column_selection = (
        feature_table.feature_names
        + feature_table.entity_names
        + [source.event_timestamp_column, source.created_timestamp_column]
    )

    missing_columns = set(column_selection) - set(mapped_source_df.columns)
    if len(missing_columns) > 0:
        raise SchemaError(
            f"{', '.join(missing_columns)} are missing for feature table {feature_table.name}."
        )

    feature_table_dtypes = dict(mapped_source_df.dtypes)
    for field in feature_table.entities + feature_table.features:
        column_type = feature_table_dtypes.get(field.name)

        if column_type != field.spark_type:
            if _type_casting_allowed(field.spark_type, column_type):
                mapped_source_df = mapped_source_df.withColumn(
                    field.name, col(field.name).cast(field.spark_type)
                )
            else:
                raise SchemaError(
                    f"{field.name} should be of {field.spark_type} type, but is {column_type} instead"
                )

    for timestamp_column in [
        source.event_timestamp_column,
        source.created_timestamp_column,
    ]:
        column_type = feature_table_dtypes.get(timestamp_column)
        if column_type != "timestamp":
            raise SchemaError(
                f"{timestamp_column} should be of timestamp type, but is {column_type} instead"
            )

    return mapped_source_df.select(
        [col(name) for name in feature_table.feature_names + feature_table.entity_names]
        + [
            col(source.event_timestamp_column).alias(EVENT_TIMESTAMP_ALIAS),
            col(source.created_timestamp_column).alias(CREATED_TIMESTAMP_ALIAS),
        ]
    )


def retrieve_historical_features(
    spark: SparkSession,
    entity_source: Source,
    feature_tables_sources: List[Source],
    feature_tables: List[FeatureTable],
) -> DataFrame:
    """Retrieve historical features based on given configurations. The argument can be either

    Args:
        spark (SparkSession): Spark session.
        entity_source (Source): Entity data source, which describe where and how to retrieve the Spark dataframe
            representing the entities.
        feature_tables_sources (Source): List of feature tables data sources, which describe where and how to
            retrieve the feature table representing the feature tables.
        feature_tables (List[FeatureTable]): List of feature table specification. The specification describes which
            features should be present in the final join result, as well as the maximum age. The order of the feature
            table must correspond to that of feature_tables_sources.

    Returns:
        DataFrame: A dataframe contains all the features specified in feature_table, where the feature columns will
            be prefixed with feature table name.

    Raises:
        SchemaError: If either the entity or feature table has missing columns or wrong column types.

    Example:
        >>> entity_source_conf = {
                "format": {"jsonClass": "ParquetFormat"},
                "path": "file:///some_dir/customer_driver_pairs.csv"),
                "options": {"inferSchema": "true", "header": "true"},
                "field_mapping": {"driver_id": "id"}
            }

        >>> feature_tables_sources_conf = [
                {
                    "format": {"json_class": "ParquetFormat"},
                    "path": "gs://some_bucket/bookings.parquet"),
                    "field_mapping": {"driver_id": "id"}
                },
                {
                    "format": {"json_class": "AvroFormat", schema_json: "..avro schema.."},
                    "path": "s3://some_bucket/transactions.avro"),
                }
            ]

        >>> feature_tables_conf = [
                {
                    "name": "bookings",
                    "entities": [{"name": "driver_id", "type": "int32"}],
                    "features": [{"name": "completed_bookings", "type": "int32"}],
                },
                {
                    "name": "transactions",
                    "entities": [{"name": "customer_id", "type": "int32"}],
                    "features": [{"name": "total_transactions", "type": "double"}],
                    "max_age": 172800
                },
            ]
    """
    entity_df = _read_and_verify_entity_df_from_source(spark, entity_source)

    feature_table_dfs = [
        _read_and_verify_feature_table_df_from_source(spark, feature_table, source,)
        for feature_table, source in zip(feature_tables, feature_tables_sources)
    ]

    expected_entities = []
    for feature_table in feature_tables:
        expected_entities.extend(feature_table.entities)

    entity_dtypes = dict(entity_df.dtypes)
    for expected_entity in expected_entities:
        if entity_dtypes.get(expected_entity.name) != expected_entity.spark_type:
            raise SchemaError(
                f"{expected_entity.name} ({expected_entity.spark_type}) is not present in the entity dataframe."
            )

    entity_df.cache()

    feature_table_dfs = [
        filter_feature_table_by_time_range(
            feature_table_df,
            feature_table,
            feature_table_source.event_timestamp_column,
            entity_df,
            entity_source.event_timestamp_column,
        )
        for feature_table_df, feature_table, feature_table_source in zip(
            feature_table_dfs, feature_tables, feature_tables_sources
        )
    ]

    return join_entity_to_feature_tables(
        entity_df,
        entity_source.event_timestamp_column,
        feature_table_dfs,
        feature_tables,
    )


def retrieve_historical_features_bq(
    spark: SparkSession,
    entity_source: BigQuerySource,
    feature_table_sources: List[BigQuerySource],
    feature_tables: List[FeatureTable],
) -> DataFrame:
    loader = FileSystemLoader("templates")
    env = Environment(loader=loader)
    extract_timestamps_template = env.get_template("extract_timestamps.jinja2")
    extract_timestamps_sql = extract_timestamps_template.render({"entity_source": entity_source})
    row = spark.read.format("bigquery") \
        .options(**entity_source.spark_read_options)\
        .load(extract_timestamps_sql).first()
    min_entity_event_timestamp = row.min_event_timestamp
    max_entity_event_timestamp = row.max_event_timestamp
    feature_views = [BigQueryFeatureView.from_feature_table(feature_table, feature_table_source, min_entity_event_timestamp, max_entity_event_timestamp)
                     for feature_table, feature_table_source in zip(feature_tables, feature_table_sources)]
    retrieval_template = env.get_template("retrieval.jinja2")
    retrieval_sql = retrieval_template.render({
        "entity_source": entity_source,
        "feature_views": feature_views,
    })

    return spark.read.format("bigquery") \
        .options(**entity_source.spark_read_options)\
        .load(retrieval_sql)


def start_job(
    spark: SparkSession,
    entity_source_conf: Dict,
    feature_tables_sources_conf: List[Dict],
    feature_tables_conf: List[Dict],
    destination_conf: Dict,
):
    feature_tables = [feature_table_from_dict(dct) for dct in feature_tables_conf]
    feature_tables_sources = [
        source_from_dict(dct) for dct in feature_tables_sources_conf
    ]
    entity_source = source_from_dict(entity_source_conf)
    if isinstance(entity_source, BigQuerySource) and all([isinstance(source, BigQuerySource) for source in
                                                          feature_tables_sources]):
        feature_tables_sources = [cast(BigQuerySource, source) for source in feature_tables_sources]
        result = retrieve_historical_features_bq(spark, entity_source, feature_tables_sources, feature_tables)
    else:
        result = retrieve_historical_features(
            spark, entity_source, feature_tables_sources, feature_tables
        )

    destination = FileDestination(**destination_conf)

    result.write.format(destination.format).mode("overwrite").save(destination.path)


def _get_args():
    parser = argparse.ArgumentParser(description="Retrieval job arguments")
    parser.add_argument(
        "--feature-tables", type=str, help="Feature table list in json string"
    )
    parser.add_argument(
        "--feature-tables-sources",
        type=str,
        help="Feature table source list in json string",
    )
    parser.add_argument(
        "--entity-source", type=str, help="Entity source in json string"
    )
    parser.add_argument(
        "--destination", type=str, help="Retrieval result destination in json string"
    )
    parser.add_argument("--checkpoint", type=str, help="Spark Checkpoint location")
    parser.add_argument(
        "--bq", type=str, help="Common bigquery configuration in json string"
    )
    return parser.parse_args()


def feature_table_from_dict(dct: Dict[str, Any]) -> FeatureTable:
    assert (
        dct.get("maxAge") is not None and dct["maxAge"] > 0
    ), "FeatureTable.maxAge must not be None and should be a positive number"

    return FeatureTable(
        name=dct["name"],
        entities=[Field(**e) for e in dct["entities"]],
        features=[Field(**f) for f in dct["features"]],
        max_age=dct["maxAge"],
        project=dct.get("project"),
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    args = _get_args()
    feature_tables_conf = json.loads(args.feature_tables)
    feature_tables_sources_conf = json.loads(args.feature_tables_sources)
    entity_source_conf = json.loads(args.entity_source)
    destination_conf = json.loads(args.destination)
    if args.checkpoint:
        spark.sparkContext.setCheckpointDir(args.checkpoint)
    if args.bq:
        bq_conf = json.loads(args.bq)
        bq_source = entity_source_conf.get("bq")
        if bq_source is not None:
            entity_source_conf["bq"]["materialization"] = bq_conf.get("materialization")
        for source_conf in feature_tables_sources_conf:
            bq_source = source_conf.get("bq")
            if bq_source is not None:
                source_conf["bq"]["materialization"] = bq_conf.get("materialization")

    try:
        start_job(
            spark,
            entity_source_conf,
            feature_tables_sources_conf,
            feature_tables_conf,
            destination_conf,
        )
    except Exception as e:
        logger.exception(e)
        raise e
    spark.stop()
