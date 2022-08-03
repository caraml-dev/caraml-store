import pytest

__all__ = (
    "caraml_store_registry"
)

@pytest.fixture(scope="session")
def caraml_store_registry(pytestconfig):
    host, port = pytestconfig.getoption("registry_url").split(":")
    return host, port
