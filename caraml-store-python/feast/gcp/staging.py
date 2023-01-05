from google.cloud import bigquery
import pandas as pd

from datetime import datetime, timedelta

from feast.data_source import BigQuerySource


def stage_entities_to_bq(
    entity_source: pd.DataFrame, project: str, dataset: str
) -> BigQuerySource:
    """
    Stores given (entity) dataframe as new table in BQ. Name of the table generated based on current time.
    Table will expire in 1 day.
    Returns BigQuerySource with reference to created table.
    """

    bq_client: bigquery.Client = bigquery.Client()
    destination = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset),
        f"_entities_{datetime.now():%Y%m%d%H%M%s}",
    )

    # prevent casting ns -> ms exception inside pyarrow
    entity_source["event_timestamp"] = entity_source["event_timestamp"].dt.floor("ms")

    load_job_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_timestamp",
        )
    )
    load_job: bigquery.LoadJob = bq_client.load_table_from_dataframe(
        entity_source,
        destination,
        job_config=load_job_config,
    )
    load_job.result()  # wait until complete

    dest_table: bigquery.Table = bq_client.get_table(destination)
    dest_table.expires = datetime.now() + timedelta(days=1)
    bq_client.update_table(dest_table, fields=["expires"])

    return BigQuerySource(
        event_timestamp_column="event_timestamp",
        table_ref=f"{destination.project}:{destination.dataset_id}.{destination.table_id}",
    )
