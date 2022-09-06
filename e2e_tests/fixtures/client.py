import os
import tempfile
import uuid
from typing import Tuple
import pytest
from feast import Client
from feast_spark import Client as SparkClient

@pytest.fixture
def feast_client(
    pytestconfig,
    caraml_store_registry: Tuple[str, int],
    caraml_store_serving: Tuple[str, int],
):
    return Client(
        core_url=f"{caraml_store_registry[0]}:{caraml_store_registry[1]}",
        serving_url=f"{caraml_store_serving[0]}:{caraml_store_serving[1]}",
        job_service_url=f"{caraml_store_registry[0]}:{caraml_store_registry[1]}",
        historical_feature_output_location=pytestconfig.getoption("historical_feature_output_location"),
        telemetry=False
    )

@pytest.fixture
def feast_spark_client(feast_client: Client) -> SparkClient:
    return SparkClient(feast_client)

@pytest.fixture(scope="session")
def global_staging_path(pytestconfig):
    if not pytestconfig.getoption(
        "staging_path", ""
    ):
        tmp_path = tempfile.mkdtemp()
        return f"file://{tmp_path}"

    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


@pytest.fixture(scope="function")
def local_staging_path(global_staging_path):
    return os.path.join(global_staging_path, str(uuid.uuid4()))
