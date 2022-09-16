
def pytest_addoption(parser):
    parser.addoption("--registry-url", action="store", default="localhost:6565")
    parser.addoption("--serving-url", action="store", default="localhost:6566")
    parser.addoption("--kafka-brokers", action="store", default="localhost:9092")
    parser.addoption("--bq-project", action="store", default="")
    parser.addoption("--historical-feature-output-location", action="store", default="file://tmp")
    parser.addopton("--store-name", action="store", default="caraml-store")
    parser.addopton("--store-type", action="store", default="BIGTABLE")


from .fixtures.client import (  # noqa
    feast_client,
    feast_spark_client,
    local_staging_path,
    global_staging_path
)

from .fixtures.external_services import (  # type: ignore # noqa
    caraml_store_registry,
    caraml_store_serving,
    kafka_server
)

from .fixtures.data import (  # noqa
    batch_source,
    bq_dataset
)
