from typing import Tuple
import pytest
from feast import Client

@pytest.fixture
def feast_client(
    pytestconfig,
    caraml_store_registry: Tuple[str, int],
):
    return Client(
        core_url=f"{caraml_store_registry[0]}:{caraml_store_registry[1]}",
        telemetry=pytestconfig.getoption("telemetry")
    )
