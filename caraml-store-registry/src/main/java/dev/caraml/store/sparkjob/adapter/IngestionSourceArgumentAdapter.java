package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

class IngestionSourceArgumentAdapter implements SparkApplicationArgumentAdapter {

  private final ObjectMapper mapper;
  private final DataSource source;

  public IngestionSourceArgumentAdapter(DataSource source) {
    this.source = source;
    this.mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  abstract static class IngestionSource {}

  @Getter
  @Setter
  @RequiredArgsConstructor
  static class FileSource extends IngestionSource {
    private final String path;
    private final Map<String, String> fieldMapping = new HashMap<>();
    private final String eventTimestampColumn;
    @Nullable String createdTimestampColumn;
    @Nullable String datePartitionColumn;
  }

  @RequiredArgsConstructor
  @Getter
  @Setter
  static class BigQuerySource extends IngestionSource {
    private final String project;
    private final String dataset;
    private final String table;
    private final String eventTimestampColumn;
    private final Map<String, String> fieldMapping;
    @Nullable String createdTimestampColumn;
    @Nullable String datePartitionColumn;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  static class KafkaSource extends IngestionSource {
    private final String bootstrapServers;
    private final String topic;
    private final DataFormat format;
    private final String eventTimestampColumn;
    private Map<String, String> fieldMapping;
  }

  static class DataFormat {}

  static class ParquetFormat extends DataFormat {}

  @Getter
  @RequiredArgsConstructor
  static class ProtoFormat extends DataFormat {
    private final String classPath;
    private final String jsonClass = "ProtoFormat";
  }

  @Getter
  @RequiredArgsConstructor
  static class AvroFormat extends DataFormat {
    private final String schemaJson;
    private final String jsonClass = "AvroFormat";
  }

  @Override
  public List<String> getArguments() {
    Map<String, IngestionSource> ingestionTypeAndSource =
        switch (source.getOptionsCase()) {
          case BIGQUERY_OPTIONS -> {
            DataSource.BigQueryOptions options = source.getBigqueryOptions();
            Pattern pattern = Pattern.compile("(?<project>[^:]+):(?<dataset>[^.]+).(?<table>.+)");
            Matcher matcher = pattern.matcher(options.getTableRef());
            matcher.find();
            if (!matcher.matches()) {
              throw new IllegalArgumentException(
                  String.format(
                      "Table ref '%s' is not in the form of <project>:<dataset>.<table>",
                      options.getTableRef()));
            }
            String project = matcher.group("project");
            String dataset = matcher.group("dataset");
            String table = matcher.group("table");
            BigQuerySource bqSource =
                new BigQuerySource(
                    project,
                    dataset,
                    table,
                    source.getEventTimestampColumn(),
                    source.getFieldMappingMap());
            if (!source.getDatePartitionColumn().isEmpty()) {
              bqSource.setDatePartitionColumn(source.getDatePartitionColumn());
            }
            if (!source.getCreatedTimestampColumn().isEmpty()) {
              bqSource.setCreatedTimestampColumn(source.getCreatedTimestampColumn());
            }
            yield Map.of("bq", bqSource);
          }

          case KAFKA_OPTIONS -> {
            DataSource.KafkaOptions options = source.getKafkaOptions();
            StreamFormat messageFormat = options.getMessageFormat();
            DataFormat dataFormat =
                messageFormat.hasProtoFormat()
                    ? new ProtoFormat(messageFormat.getProtoFormat().getClassPath())
                    : new AvroFormat(messageFormat.getAvroFormat().getSchemaJson());
            KafkaSource kafkaSource =
                new KafkaSource(
                    options.getBootstrapServers(),
                    options.getTopic(),
                    dataFormat,
                    source.getEventTimestampColumn(),
                    source.getFieldMappingMap());
            yield Map.of("kafka", kafkaSource);
          }

          default -> throw new IllegalStateException(
              "Unexpected value: " + source.getOptionsCase());
        };

    try {
      String result = mapper.writeValueAsString(ingestionTypeAndSource);
      return List.of("--source", result);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
