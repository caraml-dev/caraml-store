from pathlib import Path

from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.DataFormat_pb2 import StreamFormat as StreamFormatProto
from feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.core.OnlineStore_pb2 import OnlineStore as OnlineStoreProto, StoreType as StoreTypeProto
from feast.core.SparkOverride_pb2 import SparkOverride as SparkOverrideProto
from feast.feature_table import FeatureTable
from feast.core.FeatureTable_pb2 import FeatureTable as FeatureTableProto, FeatureTableSpec as FeatureTableSpecProto, \
    FeatureTableMeta as FeatureTableMetaProto
from feast.core.Feature_pb2 import FeatureSpec as FeatureSpecProto
from feast.types.Value_pb2 import ValueType


def test_feature_table_conversion():
    test_spec_file_path = Path(__file__).resolve().parent / "data" / "test_feature_table.yaml"
    feature_table = FeatureTable.from_yaml(str(test_spec_file_path))
    expected_feature_table_proto = FeatureTableProto(
        spec=FeatureTableSpecProto(
            name="transaction",
            entities=["merchant"],
            features=[
                FeatureSpecProto(name="avg_transaction", value_type=ValueType.DOUBLE)
            ],
            batch_source=DataSourceProto(
                type=DataSourceProto.BATCH_BIGQUERY,
                event_timestamp_column="datetime",
                bigquery_options=DataSourceProto.BigQueryOptions(
                    table_ref="project:dataset.table",
                    spark_override=SparkOverrideProto()
                ),
            ),
            stream_source=DataSourceProto(
                type=DataSourceProto.STREAM_KAFKA,
                event_timestamp_column="event_timestamp",
                kafka_options=DataSourceProto.KafkaOptions(
                    bootstrap_servers="localhost:6668",
                    topic="transaction_topic",
                    message_format=StreamFormatProto(
                        proto_format=StreamFormatProto.ProtoFormat(
                            class_path="com.transaction.Message"
                        )
                    ),
                    spark_override=SparkOverrideProto()
                )
            ),
            online_store=OnlineStoreProto(
                name="bigtable",
                type=StoreTypeProto.BIGTABLE
            )
        ),
        meta=FeatureTableMetaProto(
            created_timestamp=Timestamp()
        )

    )
    assert feature_table.to_proto() == expected_feature_table_proto
    assert feature_table == FeatureTable.from_proto(expected_feature_table_proto)
