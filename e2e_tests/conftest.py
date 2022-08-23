
def pytest_addoption(parser):
    parser.addoption("--registry-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption("--kafka-brokers", action="store", default="localhost:9092")


from .fixtures.client import (  # noqa
    feast_client,
    local_staging_path,
    global_staging_path
)

from .fixtures.external_services import (  # type: ignore # noqa
    caraml_store_registry,
    caraml_store_serving,
    kafka_server
)

from .fixtures.data import (  # noqa
    batch_source
)
