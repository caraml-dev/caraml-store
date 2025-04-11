package dev.caraml.store.sparkjob.adapter;

import static dev.caraml.store.testutils.it.DataGenerator.createParquetFormat;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import org.junit.jupiter.api.Test;

class DataSourceConverterTest {

  @Test
  void getArgumentsForBigQuerySource() throws JsonProcessingException {
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

  @Test
  void getArgumentsForMaxcomputeSource() throws JsonProcessingException {
    DataSource source =
        DataSource.newBuilder()
            .setEventTimestampColumn("event_timestamp")
            .setType(DataSource.SourceType.BATCH_MAXCOMPUTE)
            .setMaxcomputeOptions(
                DataSource.MaxComputeOptions.newBuilder()
                    .setTableRef("project.schema.table")
                    .build())
            .build();
    DataSourceConverter dataSourceConverter = new DataSourceConverter();
    String expected =
        "{\"maxcompute\":{\"project\":\"project\",\"dataset\":\"dataset\",\"table\":\"table\",\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}";
    assertEquals(expected, dataSourceConverter.convert(source));
    DataSource source2 =
        DataSource.newBuilder()
            .setEventTimestampColumn("event_timestamp")
            .setType(DataSource.SourceType.BATCH_MAXCOMPUTE)
            .setMaxcomputeOptions(
                DataSource.MaxComputeOptions.newBuilder()
                    .setTableRef("project:schema.table")
                    .build())
            .build();
    String expected2 =
        "{\"maxcompute\":{\"project\":\"project\",\"dataset\":\"dataset\",\"table\":\"table\",\"eventTimestampColumn\":\"event_timestamp\",\"fieldMapping\":{}}}";
    assertEquals(expected2, dataSourceConverter.convert(source2));
  }

  @Test
  void getArgumentsForFileSource() throws JsonProcessingException {
    DataSource source =
        DataSource.newBuilder()
            .setEventTimestampColumn("event_timestamp")
            .setType(DataSource.SourceType.BATCH_FILE)
            .setFileOptions(
                DataSource.FileOptions.newBuilder()
                    .setFileFormat(createParquetFormat())
                    .setFileUrl("gs://bucket/file")
                    .build())
            .build();
    DataSourceConverter dataSourceConverter = new DataSourceConverter();
    String expected =
        "{\"file\":{\"path\":\"gs://bucket/file\",\"format\":{\"jsonClass\":\"ParquetFormat\"},\"fieldMapping\":{},\"eventTimestampColumn\":\"event_timestamp\"}}";
    assertEquals(expected, dataSourceConverter.convert(source));
  }
}
