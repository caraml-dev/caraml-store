package dev.caraml.store.feature;

import static dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType.BATCH_BIGQUERY;
import static dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType.BATCH_FILE;
import static dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType.STREAM_KAFKA;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.caraml.store.protobuf.core.DataSourceProto;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.BigQueryOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.KafkaOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType;
import dev.caraml.store.testutils.it.DataGenerator;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DataSourceValidatorTest {

  @Test
  public void shouldErrorIfSourceTypeUnsupported() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap().get(BATCH_FILE).toBuilder().setType(SourceType.INVALID).build();
    assertThrows(UnsupportedOperationException.class, () -> DataSourceValidator.validate(badSpec));
  }

  @Test
  public void shouldPassValidSpecs() {
    getTestSpecsMap().values().forEach(DataSourceValidator::validate);
  }

  @Test
  public void shouldErrorIfBadBigQueryTableRef() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap().get(BATCH_BIGQUERY).toBuilder()
            .setBigqueryOptions(BigQueryOptions.newBuilder().setTableRef("bad:/ref").build())
            .build();
    assertThrows(IllegalArgumentException.class, () -> DataSourceValidator.validate(badSpec));
  }

  @Test
  public void shouldErrorIfBadClassPath() {
    DataSourceProto.DataSource badSpec =
        getTestSpecsMap().get(STREAM_KAFKA).toBuilder()
            .setKafkaOptions(
                KafkaOptions.newBuilder()
                    .setMessageFormat(DataGenerator.createProtoFormat(".bad^path"))
                    .build())
            .build();
    assertThrows(IllegalArgumentException.class, () -> DataSourceValidator.validate(badSpec));
  }

  private Map<SourceType, DataSourceProto.DataSource> getTestSpecsMap() {
    return Map.of(
        BATCH_FILE, DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""),
        BATCH_BIGQUERY,
            DataGenerator.createBigQueryDataSourceSpec("project:dataset.table", "ts_col", "dt_col"),
        STREAM_KAFKA,
            DataGenerator.createKafkaDataSourceSpec(
                "localhost:9092", "topic", "class.path", "ts_col"));
  }
}
