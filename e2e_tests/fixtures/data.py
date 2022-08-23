import os
import pytest

from feast import FileSource
from feast.data_format import ParquetFormat

__all__ = "batch_source"


@pytest.fixture
def batch_source(local_staging_path: str):
    return FileSource(
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created_timestamp",
        file_format=ParquetFormat(),
        file_url=os.path.join(local_staging_path, "transactions"),
    )
