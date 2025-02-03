package dev.caraml.store.sparkjob.adapter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataSourceProto;
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

class DataSourceConverter {

  private final ObjectMapper mapper;

  public DataSourceConverter() {
    this.mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  abstract static class DataSource {}

  @Getter
  @Setter
  @RequiredArgsConstructor
  static class FileSource extends DataSource {
    private final String path;
    private final DataFormat format;
    private final Map<String, String> fieldMapping = new HashMap<>();
    private final String eventTimestampColumn;
    @Nullable String createdTimestampColumn;
    @Nullable String datePartitionColumn;
  }

  @RequiredArgsConstructor
  @Getter
  @Setter
  static class BigQuerySource extends DataSource {
    private final String project;
    private final String dataset;
    private final String table;
    private final String eventTimestampColumn;
    private final Map<String, String> fieldMapping;
    @Nullable String createdTimestampColumn;
    @Nullable String datePartitionColumn;
  }

  @RequiredArgsConstructor
  @Getter
  @Setter
  static class MaxComputeSource extends DataSource {
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
  static class KafkaSource extends DataSource {
    private final String bootstrapServers;
    private final String topic;
    private final DataFormat format;
    private final String eventTimestampColumn;
    private Map<String, String> fieldMapping;
  }

  static class DataFormat {}

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

  @Getter
  @RequiredArgsConstructor
  static class ParquetFormat extends DataFormat {
    private final String jsonClass = "ParquetFormat";
  }

  public Map<String, DataSource> sourceToArgument(DataSourceProto.DataSource sourceProtobuf) {
    return switch (sourceProtobuf.getOptionsCase()) {
      case BIGQUERY_OPTIONS -> {
        DataSourceProto.DataSource.BigQueryOptions options = sourceProtobuf.getBigqueryOptions();
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
                sourceProtobuf.getEventTimestampColumn(),
                sourceProtobuf.getFieldMappingMap());
        if (!sourceProtobuf.getDatePartitionColumn().isEmpty()) {
          bqSource.setDatePartitionColumn(sourceProtobuf.getDatePartitionColumn());
        }
        if (!sourceProtobuf.getCreatedTimestampColumn().isEmpty()) {
          bqSource.setCreatedTimestampColumn(sourceProtobuf.getCreatedTimestampColumn());
        }
        yield Map.of("bq", bqSource);
      }

      case MAXCOMPUTE_OPTIONS -> {
        DataSourceProto.DataSource.MaxComputeOptions options =
                sourceProtobuf.getMaxcomputeOptions();
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
        MaxComputeSource maxComputeSource =
            new MaxComputeSource(
                project,
                dataset,
                table,
                sourceProtobuf.getEventTimestampColumn(),
                sourceProtobuf.getFieldMappingMap());
        if (!sourceProtobuf.getDatePartitionColumn().isEmpty()) {
          maxComputeSource.setDatePartitionColumn(sourceProtobuf.getDatePartitionColumn());
        }
        if (!sourceProtobuf.getCreatedTimestampColumn().isEmpty()) {
          maxComputeSource.setCreatedTimestampColumn(sourceProtobuf.getCreatedTimestampColumn());
        }
        yield Map.of("maxCompute", maxComputeSource);
      }

      case KAFKA_OPTIONS -> {
        DataSourceProto.DataSource.KafkaOptions options = sourceProtobuf.getKafkaOptions();
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
                sourceProtobuf.getEventTimestampColumn(),
                sourceProtobuf.getFieldMappingMap());
        yield Map.of("kafka", kafkaSource);
      }

      case FILE_OPTIONS -> {
        DataSourceProto.DataSource.FileOptions fileOptions = sourceProtobuf.getFileOptions();
        FileSource fileSource =
            new FileSource(
                fileOptions.getFileUrl(),
                new ParquetFormat(),
                sourceProtobuf.getEventTimestampColumn());
        if (!sourceProtobuf.getDatePartitionColumn().isEmpty()) {
          fileSource.setDatePartitionColumn(sourceProtobuf.getDatePartitionColumn());
        }
        if (!sourceProtobuf.getCreatedTimestampColumn().isEmpty()) {
          fileSource.setCreatedTimestampColumn(sourceProtobuf.getCreatedTimestampColumn());
        }
        yield Map.of("file", fileSource);
      }

      default -> throw new IllegalStateException(
          "Unexpected value: " + sourceProtobuf.getOptionsCase());
    };
  }

  public String convert(DataSourceProto.DataSource source) throws JsonProcessingException {
    return mapper.writeValueAsString(sourceToArgument(source));
  }

  public String convert(List<DataSourceProto.DataSource> sources) throws JsonProcessingException {
    return mapper.writeValueAsString(sources.stream().map(this::sourceToArgument).toList());
  }
}
