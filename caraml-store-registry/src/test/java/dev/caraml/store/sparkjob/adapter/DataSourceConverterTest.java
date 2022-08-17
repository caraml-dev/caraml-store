package dev.caraml.store.sparkjob.adapter;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import org.junit.jupiter.api.Test;

class DataSourceConverterTest {

  @Test
  void getArguments() throws JsonProcessingException {
    DataSource source =
        DataSource.newBuilder()
            .setEventTimestampColumn("event_timestamp")
            .setType(DataSource.SourceType.BATCH_BIGQUERY)
            .setBigqueryOptions(
                DataSource.BigQueryOptions.newBuilder()
                    .setTableRef("project:dataset.table")
                    .build())
            .build();
    DataSourceConverter dataSourceConverter = new DataSourceConverter();
    String expected =
        "{\"bq\":{\"project\":\"project\",\"dataset\":\"dataset\",\"table\":\"table\",\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}";
    assertEquals(expected, dataSourceConverter.convert(source));
  }
}
