package dev.caraml.store.model;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.FileFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataSourceProto;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.*;
import dev.caraml.store.util.TypeConversion;
import java.util.HashMap;
import java.util.Map;
import javax.persistence.*;
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
      }
      case BATCH_BIGQUERY -> dataSourceConfigMap.put(
          "table_ref", spec.getBigqueryOptions().getTableRef());
      case STREAM_KAFKA -> {
        dataSourceConfigMap.put("bootstrap_servers", spec.getKafkaOptions().getBootstrapServers());
        dataSourceConfigMap.put(
            "message_format", printJSON(spec.getKafkaOptions().getMessageFormat()));
        dataSourceConfigMap.put("topic", spec.getKafkaOptions().getTopic());
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

  /** Convert this DataSource to its Protobuf representation. */
  public DataSourceProto.DataSource toProto() {
    DataSourceProto.DataSource.Builder spec = DataSourceProto.DataSource.newBuilder();
    spec.setType(getType());

    // Extract source type specific options
    Map<String, String> dataSourceConfigMap =
        TypeConversion.convertJsonStringToMap(getConfigJSON());
    switch (getType()) {
      case BATCH_FILE:
        FileOptions.Builder fileOptions = FileOptions.newBuilder();
        fileOptions.setFileUrl(dataSourceConfigMap.get("file_url"));

        FileFormat.Builder fileFormat = FileFormat.newBuilder();
        parseMessage(dataSourceConfigMap.get("file_format"), fileFormat);
        fileOptions.setFileFormat(fileFormat.build());

        spec.setFileOptions(fileOptions.build());
        break;
      case BATCH_BIGQUERY:
        BigQueryOptions.Builder bigQueryOptions = BigQueryOptions.newBuilder();
        bigQueryOptions.setTableRef(dataSourceConfigMap.get("table_ref"));
        spec.setBigqueryOptions(bigQueryOptions.build());
        break;
      case STREAM_KAFKA:
        KafkaOptions.Builder kafkaOptions = KafkaOptions.newBuilder();
        kafkaOptions.setBootstrapServers(dataSourceConfigMap.get("bootstrap_servers"));
        kafkaOptions.setTopic(dataSourceConfigMap.get("topic"));

        StreamFormat.Builder messageFormat = StreamFormat.newBuilder();
        parseMessage(dataSourceConfigMap.get("message_format"), messageFormat);
        kafkaOptions.setMessageFormat(messageFormat.build());

        spec.setKafkaOptions(kafkaOptions.build());
        break;
      case STREAM_KINESIS:
        KinesisOptions.Builder kinesisOptions = KinesisOptions.newBuilder();
        kinesisOptions.setRegion(dataSourceConfigMap.get("region"));
        kinesisOptions.setStreamName(dataSourceConfigMap.get("stream_name"));

        StreamFormat.Builder recordFormat = StreamFormat.newBuilder();
        parseMessage(dataSourceConfigMap.get("record_format"), recordFormat);
        kinesisOptions.setRecordFormat(recordFormat.build());

        spec.setKinesisOptions(kinesisOptions.build());
        break;
      default:
        throw new UnsupportedOperationException(
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
