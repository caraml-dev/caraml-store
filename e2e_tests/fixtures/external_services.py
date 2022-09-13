import pytest

__all__ = (
    "caraml_store_registry",
    "caraml_store_serving",
    "kafka_server",
)


@pytest.fixture(scope="session")
def caraml_store_registry(pytestconfig):
    host, port = pytestconfig.getoption("registry_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def caraml_store_serving(pytestconfig):
    host, port = pytestconfig.getoption("serving_url").split(":")
    return host, port


@pytest.fixture(scope="session")
def kafka_server(pytestconfig):
    host, port = pytestconfig.getoption("kafka_brokers").split(":")
    return host, port
