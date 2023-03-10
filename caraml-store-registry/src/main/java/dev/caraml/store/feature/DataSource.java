package dev.caraml.store.feature;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.FileFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataSourceProto;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.BigQueryOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.FileOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.KafkaOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.SourceType;
import dev.caraml.store.protobuf.core.SparkOverrideProto.SparkOverride;
import java.util.HashMap;
import java.util.Map;
import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@javax.persistence.Entity
@Getter
@Setter(AccessLevel.PRIVATE)
@Table(name = "data_sources")
public class DataSource {
  @Column(name = "id")
  @Id
  @GeneratedValue
  private long id;

  // Type of this Data Source
  @Enumerated(EnumType.STRING)
  @Column(name = "type", nullable = false)
  private SourceType type;

  // DataSource Options
  @Column(name = "config")
  private String configJSON;

  // Field mapping between sourced fields (key) and feature fields (value).
  // Stored as serialized JSON string.
  @Column(name = "field_mapping", columnDefinition = "text")
  private String fieldMapJSON;

  @Column(name = "timestamp_column")
  private String eventTimestampColumn;

  @Column(name = "created_timestamp_column")
  private String createdTimestampColumn;

  @Column(name = "date_partition_column")
  private String datePartitionColumn;

  public DataSource() {}

  public DataSource(SourceType type) {
    this.type = type;
  }

  /**
   * Construct a DataSource from the given Protobuf representation spec
   *
   * @param spec Protobuf representation of DataSource to construct from.
   * @throws IllegalArgumentException when provided with a invalid Protobuf spec
   * @throws UnsupportedOperationException if source type is unsupported.
   */
  public static DataSource fromProto(DataSourceProto.DataSource spec) {
    DataSource source = new DataSource(spec.getType());
    // Copy source type specific options
    Map<String, String> dataSourceConfigMap = new HashMap<>();
    switch (spec.getType()) {
      case BATCH_FILE -> {
        dataSourceConfigMap.put("file_url", spec.getFileOptions().getFileUrl());
        dataSourceConfigMap.put("file_format", printJSON(spec.getFileOptions().getFileFormat()));
        populateDatasourceConfigMapWithSparkOverride(
            dataSourceConfigMap, spec.getFileOptions().getSparkOverride());
      }
      case BATCH_BIGQUERY -> {
        dataSourceConfigMap.put("table_ref", spec.getBigqueryOptions().getTableRef());
        populateDatasourceConfigMapWithSparkOverride(
            dataSourceConfigMap, spec.getBigqueryOptions().getSparkOverride());
      }
      case STREAM_KAFKA -> {
        dataSourceConfigMap.put("bootstrap_servers", spec.getKafkaOptions().getBootstrapServers());
        dataSourceConfigMap.put(
            "message_format", printJSON(spec.getKafkaOptions().getMessageFormat()));
        dataSourceConfigMap.put("topic", spec.getKafkaOptions().getTopic());
        populateDatasourceConfigMapWithSparkOverride(
            dataSourceConfigMap, spec.getKafkaOptions().getSparkOverride());
      }
      default -> throw new UnsupportedOperationException(
          String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }

    // Store DataSource mapping as serialised JSON
    source.setConfigJSON(TypeConversion.convertMapToJsonString(dataSourceConfigMap));

    // Store field mapping as serialised JSON
    source.setFieldMapJSON(TypeConversion.convertMapToJsonString(spec.getFieldMappingMap()));

    // Set timestamp mapping columns
    source.setEventTimestampColumn(spec.getEventTimestampColumn());
    source.setCreatedTimestampColumn(spec.getCreatedTimestampColumn());
    source.setDatePartitionColumn(spec.getDatePartitionColumn());

    return source;
  }

  private static SparkOverride parseDatasourceConfigMapToSparkOverride(
      Map<String, String> dataSourceConfigMap) {
    SparkOverride.Builder sparkOverride = SparkOverride.newBuilder();
    sparkOverride.setDriverMemory(dataSourceConfigMap.getOrDefault("driver_memory", ""));
    sparkOverride.setExecutorMemory(dataSourceConfigMap.getOrDefault("executor_memory", ""));
    sparkOverride.setDriverCpu(
        Integer.parseInt(dataSourceConfigMap.getOrDefault("driver_cpu", "0")));
    sparkOverride.setExecutorCpu(
        Integer.parseInt(dataSourceConfigMap.getOrDefault("executor_cpu", "0")));
    return sparkOverride.build();
  }

  private static void populateDatasourceConfigMapWithSparkOverride(
      Map<String, String> dataSourceConfigMap, SparkOverride sparkOverride) {
    if (!sparkOverride.getDriverMemory().isEmpty()) {
      dataSourceConfigMap.put("driver_memory", sparkOverride.getDriverMemory());
    }
    if (!sparkOverride.getExecutorMemory().isEmpty()) {
      dataSourceConfigMap.put("executor_memory", sparkOverride.getExecutorMemory());
    }
    if (sparkOverride.getDriverCpu() > 0) {
      dataSourceConfigMap.put(
          "driver_cpu", Integer.valueOf(sparkOverride.getDriverCpu()).toString());
    }
    if (sparkOverride.getExecutorCpu() > 0) {
      dataSourceConfigMap.put(
          "executor_cpu", Integer.valueOf(sparkOverride.getExecutorCpu()).toString());
    }
  }

  /** Convert this DataSource to its Protobuf representation. */
  public DataSourceProto.DataSource toProto() {
    DataSourceProto.DataSource.Builder spec = DataSourceProto.DataSource.newBuilder();
    spec.setType(getType());

    // Extract source type specific options
    Map<String, String> dataSourceConfigMap =
        TypeConversion.convertJsonStringToMap(getConfigJSON());
    switch (getType()) {
      case BATCH_FILE -> {
        FileOptions.Builder fileOptions = FileOptions.newBuilder();
        fileOptions.setFileUrl(dataSourceConfigMap.get("file_url"));
        FileFormat.Builder fileFormat = FileFormat.newBuilder();
        parseMessage(dataSourceConfigMap.get("file_format"), fileFormat);
        fileOptions.setFileFormat(fileFormat.build());
        fileOptions.setSparkOverride(parseDatasourceConfigMapToSparkOverride(dataSourceConfigMap));
        spec.setFileOptions(fileOptions.build());
      }
      case BATCH_BIGQUERY -> {
        BigQueryOptions.Builder bigQueryOptions = BigQueryOptions.newBuilder();
        bigQueryOptions.setTableRef(dataSourceConfigMap.get("table_ref"));
        bigQueryOptions.setSparkOverride(
            parseDatasourceConfigMapToSparkOverride(dataSourceConfigMap));
        spec.setBigqueryOptions(bigQueryOptions.build());
      }
      case STREAM_KAFKA -> {
        KafkaOptions.Builder kafkaOptions = KafkaOptions.newBuilder();
        kafkaOptions.setBootstrapServers(dataSourceConfigMap.get("bootstrap_servers"));
        kafkaOptions.setTopic(dataSourceConfigMap.get("topic"));
        StreamFormat.Builder messageFormat = StreamFormat.newBuilder();
        parseMessage(dataSourceConfigMap.get("message_format"), messageFormat);
        kafkaOptions.setMessageFormat(messageFormat.build());
        kafkaOptions.setSparkOverride(parseDatasourceConfigMapToSparkOverride(dataSourceConfigMap));
        spec.setKafkaOptions(kafkaOptions.build());
      }
      default -> throw new UnsupportedOperationException(
          String.format("Unsupported Feature Store Type: %s", getType()));
    }

    // Parse field mapping and options from JSON
    spec.putAllFieldMapping(TypeConversion.convertJsonStringToMap(getFieldMapJSON()));

    spec.setEventTimestampColumn(getEventTimestampColumn());
    spec.setCreatedTimestampColumn(getCreatedTimestampColumn());
    spec.setDatePartitionColumn(getDatePartitionColumn());

    return spec.build();
  }

  public Map<String, String> getFieldsMap() {
    return TypeConversion.convertJsonStringToMap(getFieldMapJSON());
  }

  @Override
  public int hashCode() {
    return toProto().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSource other = (DataSource) o;
    return this.toProto().equals(other.toProto());
  }

  /** Print the given Message into its JSON string representation */
  private static String printJSON(MessageOrBuilder message) {
    try {
      return JsonFormat.printer().print(message);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unexpected exception convering Proto to JSON", e);
    }
  }

  /** Parse the given Message in JSON representation into the given Message Builder */
  private static void parseMessage(String json, Message.Builder message) {
    try {
      JsonFormat.parser().merge(json, message);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unexpected exception convering JSON to Proto", e);
    }
  }
}
