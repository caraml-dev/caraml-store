package dev.caraml.store.testutils.it;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.core.DataFormatProto.FileFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.FileFormat.ParquetFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat.AvroFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat.ProtoFormat;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.FileOptions;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource.KafkaOptions;
import dev.caraml.store.protobuf.core.EntityProto;
import dev.caraml.store.protobuf.core.FeatureProto;
import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import dev.caraml.store.protobuf.types.ValueProto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Triple;

public class DataGenerator {
  // projectName, featureName, exclude
  static Triple<String, String, Boolean> defaultSubscription = Triple.of("*", "*", false);

  public static Triple<String, String, Boolean> getDefaultSubscription() {
    return defaultSubscription;
  }

  public static String valueToString(ValueProto.Value v) {

    return switch (v.getValCase()) {
      case STRING_VAL -> v.getStringVal();
      case INT64_VAL -> String.valueOf(v.getInt64Val());
      case INT32_VAL -> String.valueOf(v.getInt32Val());
      case BYTES_VAL -> v.getBytesVal().toString();
      default -> throw new RuntimeException("Type is not supported to be entity");
    };
  }

  public static EntityProto.EntitySpec createEntitySpec(
      String name,
      String description,
      ValueProto.ValueType.Enum valueType,
      Map<String, String> labels) {
    return EntityProto.EntitySpec.newBuilder()
        .setName(name)
        .setDescription(description)
        .setValueType(valueType)
        .putAllLabels(labels)
        .build();
  }

  public static FeatureProto.FeatureSpec createFeatureSpec(
      String name, ValueProto.ValueType.Enum valueType, Map<String, String> labels) {
    return FeatureProto.FeatureSpec.newBuilder()
        .setName(name)
        .setValueType(valueType)
        .putAllLabels(labels)
        .build();
  }

  // Create a Feature Table spec without DataSources configured.
  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      Map<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> labels) {

    return FeatureTableSpec.newBuilder()
        .setName(name)
        .addAllEntities(entities)
        .addAllFeatures(
            features.entrySet().stream()
                .map(
                    entry ->
                        FeatureSpec.newBuilder()
                            .setName(entry.getKey())
                            .setValueType(entry.getValue())
                            .putAllLabels(labels)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(3600).build())
        .setBatchSource(
            DataSource.newBuilder()
                .setEventTimestampColumn("ts")
                .setType(DataSource.SourceType.BATCH_FILE)
                .setFileOptions(
                    FileOptions.newBuilder()
                        .setFileFormat(
                            FileFormat.newBuilder()
                                .setParquetFormat(ParquetFormat.newBuilder().build())
                                .build())
                        .setFileUrl("/dev/null")
                        .build())
                .build())
        .putAllLabels(labels)
        .build();
  }

  public static FeatureTableSpec createFeatureTableSpec(
      String name,
      List<String> entities,
      ImmutableMap<String, ValueProto.ValueType.Enum> features,
      int maxAgeSecs,
      Map<String, String> labels) {

    return FeatureTableSpec.newBuilder()
        .setName(name)
        .addAllEntities(entities)
        .addAllFeatures(
            features.entrySet().stream()
                .map(
                    entry ->
                        FeatureSpec.newBuilder()
                            .setName(entry.getKey())
                            .setValueType(entry.getValue())
                            .putAllLabels(labels)
                            .build())
                .collect(Collectors.toList()))
        .setMaxAge(Duration.newBuilder().setSeconds(maxAgeSecs).build())
        .putAllLabels(labels)
        .build();
  }

  public static DataSource createFileDataSourceSpec(
      String fileURL, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_FILE)
        .setFileOptions(
            FileOptions.newBuilder()
                .setFileFormat(createParquetFormat())
                .setFileUrl(fileURL)
                .build())
        .setEventTimestampColumn(timestampColumn)
        .setDatePartitionColumn(datePartitionColumn)
        .build();
  }

  public static DataSource createBigQueryDataSourceSpec(
      String bigQueryTableRef, String timestampColumn, String datePartitionColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.BATCH_BIGQUERY)
        .setBigqueryOptions(
            DataSource.BigQueryOptions.newBuilder().setTableRef(bigQueryTableRef).build())
        .setEventTimestampColumn(timestampColumn)
        .setDatePartitionColumn(datePartitionColumn)
        .build();
  }

  public static DataSource createKafkaDataSourceSpec(
      String servers, String topic, String classPath, String timestampColumn) {
    return DataSource.newBuilder()
        .setType(DataSource.SourceType.STREAM_KAFKA)
        .setKafkaOptions(
            KafkaOptions.newBuilder()
                .setTopic(topic)
                .setBootstrapServers(servers)
                .setMessageFormat(createProtoFormat("class.path"))
                .build())
        .setEventTimestampColumn(timestampColumn)
        .build();
  }

  public static ValueProto.Value createEmptyValue() {
    return ValueProto.Value.newBuilder().build();
  }

  public static ValueProto.Value createStrValue(String val) {
    return ValueProto.Value.newBuilder().setStringVal(val).build();
  }

  public static ValueProto.Value createDoubleValue(double value) {
    return ValueProto.Value.newBuilder().setDoubleVal(value).build();
  }

  public static ValueProto.Value createInt32Value(int value) {
    return ValueProto.Value.newBuilder().setInt32Val(value).build();
  }

  public static ValueProto.Value createInt64Value(long value) {
    return ValueProto.Value.newBuilder().setInt64Val(value).build();
  }

  public static FileFormat createParquetFormat() {
    return FileFormat.newBuilder().setParquetFormat(ParquetFormat.getDefaultInstance()).build();
  }

  public static StreamFormat createAvroFormat(String schemaJSON) {
    return StreamFormat.newBuilder()
        .setAvroFormat(AvroFormat.newBuilder().setSchemaJson(schemaJSON).build())
        .build();
  }

  public static StreamFormat createProtoFormat(String classPath) {
    return StreamFormat.newBuilder()
        .setProtoFormat(ProtoFormat.newBuilder().setClassPath(classPath).build())
        .build();
  }

  public static OnlineStoreProto.OnlineStore createOnlineStore(String name) {
    return OnlineStoreProto.OnlineStore.newBuilder()
        .setName(name)
        .setType(OnlineStoreProto.StoreType.REDIS)
        .setDescription("A dummy online store")
        .build();
  }

  public static EntityRow createEntityRow(
      String entityName, ValueProto.Value entityValue, long seconds) {
    return EntityRow.newBuilder()
        .setTimestamp(Timestamp.newBuilder().setSeconds(seconds))
        .putFields(entityName, entityValue)
        .build();
  }

  public static FeatureReference createFeatureReference(
      String featureTableName, String featureName) {
    return FeatureReference.newBuilder()
        .setFeatureTable(featureTableName)
        .setName(featureName)
        .build();
  }
}
