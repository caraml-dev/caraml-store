import pathlib
from datetime import datetime
from os import path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from scripts.historical_feature_retrieval_job import (
    FeatureTable,
    Field,
    SchemaError,
    as_of_join,
    filter_feature_table_by_time_range,
    join_entity_to_feature_tables,
    retrieve_historical_features, source_from_dict, feature_table_from_dict,
)


@pytest.yield_fixture(scope="module")
def spark(pytestconfig):
    spark_session = (
        SparkSession.builder.appName("As of join test").master("local").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def single_entity_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )


@pytest.fixture
def composite_entity_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
        ]
    )


@pytest.fixture
def customer_feature_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("daily_transactions", FloatType()),
        ]
    )


@pytest.fixture
def customer_feature_schema_with_double_type():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("daily_transactions", DoubleType()),
        ]
    )


@pytest.fixture
def driver_feature_schema():
    return StructType(
        [
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("completed_bookings", IntegerType()),
        ]
    )


@pytest.fixture
def rating_feature_schema():
    return StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("created_timestamp", TimestampType()),
            StructField("customer_rating", FloatType()),
            StructField("driver_rating", FloatType()),
        ]
    )


def assert_dataframe_equal(left: DataFrame, right: DataFrame):
    is_column_equal = set(left.columns) == set(right.columns)

    if not is_column_equal:
        print(f"Column not equal. Left: {left.columns}, Right: {right.columns}")
    assert is_column_equal

    is_content_equal = (
        left.exceptAll(right).count() == 0 and right.exceptAll(left).count() == 0
    )
    if not is_content_equal:
        print("Rows are different.")
        print("Left:")
        left.show()
        print("Right:")
        right.show()

    assert is_content_equal


def test_join_with_max_age(
    spark: SparkSession,
    single_entity_schema: StructType,
    customer_feature_schema: StructType,
):
    entity_data = [
        (1001, datetime(year=2020, month=9, day=1)),
        (1001, datetime(year=2020, month=9, day=3)),
        (2001, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), single_entity_schema
    )

    feature_table_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            200.0,
        ),
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), customer_feature_schema
    )
    feature_table = FeatureTable(
        name="transactions",
        features=[Field("daily_transactions", "double")],
        entities=[Field("customer_id", "int32")],
        max_age=86400,
    )

    feature_table_df = filter_feature_table_by_time_range(
        feature_table_df,
        feature_table,
        "event_timestamp",
        entity_df,
        "event_timestamp",
    )

    joined_df = as_of_join(
        entity_df, "event_timestamp", feature_table_df, feature_table,
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, datetime(year=2020, month=9, day=1), 100.0,),
        (1001, datetime(year=2020, month=9, day=3), None),
        (2001, datetime(year=2020, month=9, day=2), 200.0,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_join_with_composite_entity(
    spark: SparkSession,
    composite_entity_schema: StructType,
    rating_feature_schema: StructType,
):
    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1)),
        (1001, 8002, datetime(year=2020, month=9, day=3)),
        (1001, 8003, datetime(year=2020, month=9, day=1)),
        (2001, 8001, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    feature_table_data = [
        (
            1001,
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            3.0,
            5.0,
        ),
        (
            1001,
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            4.0,
            3.0,
        ),
        (
            2001,
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            4.0,
            4.5,
        ),
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), rating_feature_schema,
    )
    feature_table = FeatureTable(
        name="ratings",
        features=[Field("customer_rating", "double"), Field("driver_rating", "double")],
        entities=[Field("customer_id", "int32"), Field("driver_id", "int32")],
        max_age=86400,
    )
    feature_table_df = filter_feature_table_by_time_range(
        feature_table_df,
        feature_table,
        "event_timestamp",
        entity_df,
        "event_timestamp",
    )
    joined_df = as_of_join(
        entity_df, "event_timestamp", feature_table_df, feature_table,
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("ratings__customer_rating", FloatType()),
            StructField("ratings__driver_rating", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=1), 3.0, 5.0,),
        (1001, 8002, datetime(year=2020, month=9, day=3), None, None),
        (1001, 8003, datetime(year=2020, month=9, day=1), None, None),
        (2001, 8001, datetime(year=2020, month=9, day=2), 4.0, 4.5,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_select_subset_of_columns_as_entity_primary_keys(
    spark: SparkSession,
    composite_entity_schema: StructType,
    customer_feature_schema: StructType,
):
    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2)),
        (2001, 8002, datetime(year=2020, month=9, day=2)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    feature_table_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=2),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            400.0,
        ),
    ]
    feature_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(feature_table_data), customer_feature_schema
    )
    feature_table = FeatureTable(
        name="transactions",
        features=[Field("daily_transactions", "double")],
        entities=[Field("customer_id", "int32")],
        max_age=86400,
    )
    feature_table_df = filter_feature_table_by_time_range(
        feature_table_df,
        feature_table,
        "event_timestamp",
        entity_df,
        "event_timestamp",
    )
    joined_df = as_of_join(
        entity_df, "event_timestamp", feature_table_df, feature_table,
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )
    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2), 100.0,),
        (2001, 8002, datetime(year=2020, month=9, day=2), 400.0,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_multiple_join(
    spark: SparkSession,
    composite_entity_schema: StructType,
    customer_feature_schema: StructType,
    driver_feature_schema: StructType,
):

    entity_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2)),
        (1001, 8002, datetime(year=2020, month=9, day=2)),
        (2001, 8002, datetime(year=2020, month=9, day=3)),
    ]
    entity_df = spark.createDataFrame(
        spark.sparkContext.parallelize(entity_data), composite_entity_schema
    )

    customer_table_data = [
        (
            1001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            100.0,
        ),
        (
            2001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            200.0,
        ),
    ]
    customer_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(customer_table_data), customer_feature_schema
    )
    customer_table = FeatureTable(
        name="transactions",
        features=[Field("daily_transactions", "double")],
        entities=[Field("customer_id", "int32")],
        max_age=86400,
    )
    customer_table_df = filter_feature_table_by_time_range(
        customer_table_df,
        customer_table,
        "event_timestamp",
        entity_df,
        "event_timestamp",
    )

    driver_table_data = [
        (
            8001,
            datetime(year=2020, month=8, day=31),
            datetime(year=2020, month=8, day=31),
            200,
        ),
        (
            8001,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            300,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=1),
            600,
        ),
        (
            8002,
            datetime(year=2020, month=9, day=1),
            datetime(year=2020, month=9, day=2),
            500,
        ),
    ]
    driver_table_df = spark.createDataFrame(
        spark.sparkContext.parallelize(driver_table_data), driver_feature_schema
    )

    driver_table = FeatureTable(
        name="bookings",
        features=[Field("completed_bookings", "int32")],
        entities=[Field("driver_id", "int32")],
        max_age=7 * 86400,
    )
    driver_table_df = filter_feature_table_by_time_range(
        driver_table_df, driver_table, "event_timestamp", entity_df, "event_timestamp",
    )
    joined_df = join_entity_to_feature_tables(
        entity_df,
        "event_timestamp",
        [customer_table_df, driver_table_df],
        [customer_table, driver_table],
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
            StructField("bookings__completed_bookings", IntegerType()),
        ]
    )

    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2), 100.0, 300,),
        (1001, 8002, datetime(year=2020, month=9, day=2), 100.0, 500,),
        (2001, 8002, datetime(year=2020, month=9, day=3), None, 500,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_historical_feature_retrieval(spark: SparkSession):
    test_data_dir = path.join(pathlib.Path(__file__).parent.absolute(), "data")
    entity_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'customer_driver_pairs.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'bookings.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "createdTimestampColumn": "created_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    transaction_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'transactions.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "createdTimestampColumn": "created_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_table = feature_table_from_dict({
        "name": "bookings",
        "entities": [{"name": "driver_id", "type": "int32"}],
        "features": [{"name": "completed_bookings", "type": "int32"}],
        "maxAge": 365 * 86400,
    })
    transaction_table = feature_table_from_dict({
        "name": "transactions",
        "entities": [{"name": "customer_id", "type": "int32"}],
        "features": [{"name": "daily_transactions", "type": "double"}],
        "maxAge": 86400,
    })

    joined_df = retrieve_historical_features(
        spark,
        entity_source,
        [transaction_source, booking_source],
        [transaction_table, booking_table],
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("driver_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
            StructField("bookings__completed_bookings", IntegerType()),
        ]
    )

    expected_joined_data = [
        (1001, 8001, datetime(year=2020, month=9, day=2), 100.0, 300,),
        (1001, 8002, datetime(year=2020, month=9, day=2), 100.0, 500,),
        (1001, 8002, datetime(year=2020, month=9, day=3), None, 500,),
        (2001, 8002, datetime(year=2020, month=9, day=3), None, 500,),
        (2001, 8002, datetime(year=2020, month=9, day=4), None, 500,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_historical_feature_retrieval_with_mapping(spark: SparkSession):
    test_data_dir = path.join(pathlib.Path(__file__).parent.absolute(), "data")
    entity_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'column_mapping_test_entity.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "fieldMapping": {"customer_id": "id"},
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'column_mapping_test_feature.csv')}",
            "eventTimestampColumn": "datetime",
            "createdTimestampColumn": "created_datetime",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_table = feature_table_from_dict({
        "name": "bookings",
        "entities": [{"name": "customer_id", "type": "int32"}],
        "features": [{"name": "total_bookings", "type": "int32"}],
        "maxAge": 86400,
    })

    joined_df = retrieve_historical_features(
        spark, entity_source, [booking_source], [booking_table],
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("bookings__total_bookings", IntegerType()),
        ]
    )

    expected_joined_data = [
        (1001, datetime(year=2020, month=9, day=2), 200),
        (1001, datetime(year=2020, month=9, day=3), 200),
        (2001, datetime(year=2020, month=9, day=4), 600),
        (2001, datetime(year=2020, month=9, day=4), 600),
        (3001, datetime(year=2020, month=9, day=4), 700),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)


def test_historical_feature_retrieval_with_schema_errors(spark: SparkSession):
    test_data_dir = path.join(pathlib.Path(__file__).parent.absolute(), "data")
    entity_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'customer_driver_pairs.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    entity_source_missing_timestamp = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'customer_driver_pairs.csv')}",
            "eventTimestampColumn": "datetime",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    entity_source_missing_entity = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'customers.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })

    booking_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'bookings.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "createdTimestampColumn": "created_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_source_missing_timestamp = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'bookings.csv')}",
            "eventTimestampColumn": "datetime",
            "createdTimestampColumn": "created_datetime",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    booking_table = feature_table_from_dict({
        "name": "bookings",
        "entities": [{"name": "driver_id", "type": "int32"}],
        "features": [{"name": "completed_bookings", "type": "int32"}],
        "maxAge": 86400,
    })
    booking_table_missing_features = feature_table_from_dict({
        "name": "bookings",
        "entities": [{"name": "driver_id", "type": "int32"}],
        "features": [{"name": "nonexist_feature", "type": "int32"}],
        "maxAge": 86400,
    })

    with pytest.raises(SchemaError):
        retrieve_historical_features(
            spark, entity_source_missing_timestamp, [booking_source], [booking_table],
        )

    with pytest.raises(SchemaError):
        retrieve_historical_features(
            spark, entity_source, [booking_source_missing_timestamp], [booking_table],
        )

    with pytest.raises(SchemaError):
        retrieve_historical_features(
            spark, entity_source, [booking_source], [booking_table_missing_features],
        )

    with pytest.raises(SchemaError):
        retrieve_historical_features(
            spark, entity_source_missing_entity, [booking_source], [booking_table],
        )


def test_implicit_type_conversion(spark: SparkSession,):
    test_data_dir = path.join(pathlib.Path(__file__).parent.absolute(), "data")
    entity_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'single_customer.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    transaction_source = source_from_dict({
        "file": {
            "format": {"jsonClass": "CSVFormat"},
            "path": f"file://{path.join(test_data_dir,  'transactions.csv')}",
            "eventTimestampColumn": "event_timestamp",
            "createdTimestampColumn": "created_timestamp",
            "options": {"inferSchema": "true", "header": "true"},
        }
    })
    transaction_table = feature_table_from_dict({
        "name": "transactions",
        "entities": [{"name": "customer_id", "type": "int32"}],
        "features": [{"name": "daily_transactions", "type": "float"}],
        "maxAge": 86400,
    })

    joined_df = retrieve_historical_features(
        spark, entity_source, [transaction_source], [transaction_table],
    )

    expected_joined_schema = StructType(
        [
            StructField("customer_id", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("transactions__daily_transactions", FloatType()),
        ]
    )

    expected_joined_data = [
        (1001, datetime(year=2020, month=9, day=2), 100.0,),
    ]
    expected_joined_df = spark.createDataFrame(
        spark.sparkContext.parallelize(expected_joined_data), expected_joined_schema
    )

    assert_dataframe_equal(joined_df, expected_joined_df)