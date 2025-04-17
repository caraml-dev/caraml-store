import os
import uuid
import pandas as pd
from datetime import datetime, timedelta
from feast.data_source import MaxComputeSource
from odps import ODPS
from odps import df

# Create table schema based on DataFrame columns
# Map pandas dtypes to MaxCompute types
dtype_to_mc_type = {
    "object": "STRING",
    "string": "STRING",
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
}


def stage_entities_to_maxcompute(
    entity_source: pd.DataFrame,
    project: str,
    schema: str,
    timestamp_column: str = "event_timestamp",
    endpoint: str = "",
    access_key: str = "",
    secret_key: str = "",
) -> MaxComputeSource:
    """
    Stores given (entity) dataframe as new table in MaxCompute.
    Table will expire in 1 day.
    Returns MaxComputeSource with reference to created table.

    Args:
        entity_source: DataFrame containing entity data
        project: MaxCompute project name
        schema: MaxCompute schema name
        table_name: Name for the table to be created
        endpoint: MaxCompute endpoint URL (defaults to env var MAXCOMPUTE_ENDPOINT)
        access_key: MaxCompute access key (defaults to env var MAXCOMPUTE_ACCESS_KEY)
        secret_key: MaxCompute secret key (defaults to env var MAXCOMPUTE_SECRET_KEY)

    Returns:
        MaxComputeSource with reference to created table
    """
    # Get credentials from args or environment variables
    endpoint = endpoint or os.getenv("MAXCOMPUTE_ENDPOINT")
    access_key = access_key or os.getenv("MAXCOMPUTE_ACCESS_KEY")
    secret_key = secret_key or os.getenv("MAXCOMPUTE_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "MaxCompute credentials not provided. Either pass access_key and secret_key "
            "as arguments or set MAXCOMPUTE_ACCESS_KEY and MAXCOMPUTE_SECRET_KEY "
            "environment variables."
        )

    # Initialize MaxCompute client
    odps_client = ODPS(access_key, secret_key, project, endpoint=endpoint)

    entity_source[timestamp_column] = (
        entity_source[timestamp_column].dt.tz_convert("UTC").dt.tz_localize(None)
    )
    # Prevent casting ns -> ms exception inside pyarrow
    entity_source[timestamp_column] = entity_source[timestamp_column].dt.floor("ms")

    # Create table with timestamp partitioning
    full_table_name = f"{project}.{schema}.entities_{uuid.uuid4()}".replace("-", "_")
    print("entity full_table_name: ", full_table_name)

    # Create columns for the table schema
    columns = []
    for col_name, dtype in entity_source.dtypes.items():
        if col_name == timestamp_column:
            mc_type = "TIMESTAMP"
        else:
            mc_type = dtype_to_mc_type.get(str(dtype), "STRING")
        columns.append(f"{col_name} {mc_type}")

    timestamp_column = "event_timestamp"

    # Create the table
    mc_event_timestamp_column = f"{timestamp_column}_pt"
    create_table_query = f"create table {full_table_name} ( {', '.join(columns)} )  auto partitioned by (trunc_time({timestamp_column}, 'day') as {mc_event_timestamp_column}) lifecycle 1"
    print(f"query: {create_table_query}")
    odps_client.execute_sql(create_table_query)

    # Write DataFrame to MaxCompute table
    odps_df = df.DataFrame(entity_source)
    odps_df.persist(
        full_table_name,
        partition=f"{mc_event_timestamp_column}={timestamp_column}",
        lifecycle=1,
    )

    return MaxComputeSource(
        event_timestamp_column="event_timestamp",
        table_ref=f"{full_table_name}",
    )