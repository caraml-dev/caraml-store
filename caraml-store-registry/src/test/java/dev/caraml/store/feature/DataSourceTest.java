package dev.caraml.store.feature;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import dev.caraml.store.protobuf.core.DataSourceProto;
import dev.caraml.store.testutils.it.DataGenerator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class DataSourceTest {
  @Test
  public void shouldSerializeFieldMappingAsJSON() {
    Map<String, String> expectedMap = Map.of("test", "value");

    getTestSpecs()
        .forEach(
            spec -> {
              DataSource source =
                  DataSource.fromProto(spec.toBuilder().putAllFieldMapping(expectedMap).build());
              Map<String, String> actualMap = source.getFieldsMap();
              assertThat(actualMap, equalTo(actualMap));
            });
  }

  @Test
  public void shouldFromProtoBeReversableWithToProto() {
    getTestSpecs()
        .forEach(
            expectedSpec -> {
              DataSourceProto.DataSource actualSpec = DataSource.fromProto(expectedSpec).toProto();
              assertThat(actualSpec, equalTo(expectedSpec));
            });
  }

  private List<DataSourceProto.DataSource> getTestSpecs() {
    return List.of(
        DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""),
        DataGenerator.createKafkaDataSourceSpec("localhost:9092", "topic", "class.path", "ts_col"),
        DataGenerator.createBigQueryDataSourceSpec("project:dataset.table", "ts_col", "dt_col"));
  }
}
