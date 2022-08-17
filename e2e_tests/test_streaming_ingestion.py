import json
import os
import time
import uuid

import numpy as np
import pandas as pd
from google.protobuf.duration_pb2 import Duration

from feast import (
    Client,
    Entity,
    Feature,
    FeatureTable,
    FileSource,
    KafkaSource,
    ValueType,
)
from feast.data_format import AvroFormat, ParquetFormat
from feast.online_store import OnlineStore
from feast.wait import wait_retry_backoff
from e2e_tests.utils.kafka import check_consumer_exist, ingest_and_retrieve


def generate_data():
    df = pd.DataFrame(columns=["s2id", "unique_drivers", "event_timestamp"])
    df["s2id"] = np.random.choice(999999, size=100, replace=False)
    df["unique_drivers"] = np.random.randint(0, 1000, 100)
    df["event_timestamp"] = pd.to_datetime(
        np.random.randint(int(time.time()), int(time.time()) + 3600, 100), unit="s"
    )
    df["date"] = df["event_timestamp"].dt.date

    return df

def test_streaming_ingestion_bigtable(
    feast_client: Client,
    kafka_server,
    pytestconfig,
):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)
    kafka_broker = f"{kafka_server[0]}:{kafka_server[1]}"
    topic_name = f"avro-{uuid.uuid4()}"

    feature_table = FeatureTable(
        name="drivers_stream",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        max_age=Duration(seconds=3600),
        batch_source=FileSource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="event_timestamp",
            file_format=ParquetFormat(),
            file_url=os.path.join("/tmp/batch-storage"),
        ),
        stream_source=KafkaSource(
            event_timestamp_column="event_timestamp",
            bootstrap_servers=kafka_broker,
            message_format=AvroFormat(avro_schema()),
            topic=topic_name,
        ),
        online_store=OnlineStore(
            name="feast-bigtable",
            store_type="BIGTABLE",
            description="Test online store"
        ),
    )

    # Register OnlineStore
    feast_client.register_online_store(OnlineStore(
        name="feast-bigtable",
        store_type="BIGTABLE",
        description="Test online store"
    ))

    feast_client.apply(entity)
    feast_client.apply(feature_table)

    wait_retry_backoff(
        lambda: (None, check_consumer_exist(kafka_broker, topic_name)), 300
    )

    test_data = generate_data()[["s2id", "unique_drivers", "event_timestamp"]]

    try:
        ingested = ingest_and_retrieve(
            feast_client,
            test_data,
            avro_schema_json=avro_schema(),
            topic_name=topic_name,
            kafka_broker=kafka_broker,
            entity_rows=[{"s2id": s2_id} for s2_id in test_data["s2id"].tolist()],
            feature_names=["drivers_stream:unique_drivers"],
        )
    finally:
        feast_client.delete_feature_table(feature_table.name)

    pd.testing.assert_frame_equal(
        ingested[["s2id", "drivers_stream:unique_drivers"]],
        test_data[["s2id", "unique_drivers"]].rename(
            columns={"unique_drivers": "drivers_stream:unique_drivers"}
        ),
    )


def avro_schema():
    return json.dumps(
        {
            "type": "record",
            "name": "TestMessage",
            "fields": [
                {"name": "s2id", "type": "long"},
                {"name": "unique_drivers", "type": "long"},
                {
                    "name": "event_timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-micros"},
                },
            ],
        }
    )
