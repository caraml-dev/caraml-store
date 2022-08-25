import time
import pytest

from feast import BigQuerySource
from google.cloud import bigquery

from _pytest.fixtures import FixtureRequest
from datetime import datetime


__all__ = ("bq_dataset", "batch_source")

@pytest.fixture(scope="session")
def bq_dataset(pytestconfig):
    client = bigquery.Client(project=pytestconfig.getoption("bq_project"))
    timestamp = int(time.time())
    name = f"feast_e2e_{timestamp}"
    client.create_dataset(name)
    yield name
    client.delete_dataset(name, delete_contents=True)

@pytest.fixture
def batch_source(local_staging_path: str, pytestconfig, request: FixtureRequest):
    bq_project = "gods-dev"
    bq_dataset = request.getfixturevalue("bq_dataset")
    return BigQuerySource(
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
        table_ref=f"{bq_project}:{bq_dataset}.source_{datetime.now():%Y%m%d%H%M%s}",
    )
