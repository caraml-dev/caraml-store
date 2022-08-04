import pytest
from feast.client import Client
from feast.online_store import OnlineStore


@pytest.fixture
def online_store():
    return OnlineStore(name="test-store",
                       store_type="BIGTABLE",
                       description="Test online store"
                       )


def test_register_online_store(
        feast_client: Client,
        online_store: OnlineStore
):

    # Register online store
    feast_client.register_online_store(online_store)

    # Get online store
    assert feast_client.get_online_store(name=online_store.name) == online_store


def test_archive_online_store(
        feast_client: Client,
        online_store: OnlineStore
):

    assert online_store.name in [store.name for store in feast_client.list_online_stores()]

    # Archive online store
    feast_client.archive_online_store(name=online_store.name)

    # Check that online store should not be listed after archiving
    assert online_store.name not in [store.name for store in feast_client.list_online_stores()]
