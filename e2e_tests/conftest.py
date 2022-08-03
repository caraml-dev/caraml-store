
def pytest_addoption(parser):
    parser.addoption("--registry-url", action="store", default="localhost:6565")
    parser.addoption("--telemetry", action="store", default=False)


from .fixtures.client import (  # noqa
    feast_client
)

from .fixtures.external_services import (  # type: ignore # noqa
    caraml_store_registry
)
