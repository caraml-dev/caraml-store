from typing import Tuple
import pytest
from feast import Client

@pytest.fixture
def feast_client(
    pytestconfig,
    caraml_store_registry: Tuple[str, int],
    caraml_store_serving: Tuple[str, int],
):
    return Client(
        core_url=f"{caraml_store_registry[0]}:{caraml_store_registry[1]}",
        serving_url=f"{caraml_store_serving[0]}:{caraml_store_serving[1]}",
        telemetry=False
    )
