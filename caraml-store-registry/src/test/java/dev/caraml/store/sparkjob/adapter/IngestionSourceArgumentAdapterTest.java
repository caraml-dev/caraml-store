package dev.caraml.store.sparkjob.adapter;

import static org.junit.jupiter.api.Assertions.*;

import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import org.junit.jupiter.api.Test;

class IngestionSourceArgumentAdapterTest {

  @Test
  void getArguments() {
    DataSource source =
        DataSource.newBuilder()
            .setEventTimestampColumn("event_timestamp")
            .setType(DataSource.SourceType.BATCH_BIGQUERY)
            .setBigqueryOptions(
                DataSource.BigQueryOptions.newBuilder()
                    .setTableRef("project:dataset.table")
                    .build())
            .build();
    IngestionSourceArgumentAdapter ingestionSourceArgumentAdapter =
        new IngestionSourceArgumentAdapter(source);
    assertEquals("", ingestionSourceArgumentAdapter.getArguments().get(1));
  }
}
