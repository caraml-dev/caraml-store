package dev.caraml.serving.store.bigtable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import dev.caraml.serving.store.Feature;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import dev.caraml.store.testutils.it.DataGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class BigTableOnlineRetrieverTest {

  static final String PROJECT_ID = "test-project";
  static final String INSTANCE_ID = "test-instance";
  static final Integer BIGTABLE_EMULATOR_PORT = 8086;
  static final String FEAST_PROJECT = "default";
  static BigtableDataClient client;
  static Connection hbaseClient;
  static BigtableTableAdminClient adminClient;

  @Container
  public static GenericContainer<?> bigtableEmulator =
      new GenericContainer<>("google/cloud-sdk:latest")
          .withEnv(
              "GOOGLE_APPLICATION_CREDENTIALS",
              "/Users/user/.config/gcloud/application_default_credentials.json")
          .withCommand(
              "gcloud",
              "beta",
              "emulators",
              "bigtable",
              "start",
              "--host-port",
              "0.0.0.0:" + BIGTABLE_EMULATOR_PORT)
          .withExposedPorts(BIGTABLE_EMULATOR_PORT);

  @BeforeAll
  public static void setup() throws IOException {
    client =
        BigtableDataClient.create(
            BigtableDataSettings.newBuilderForEmulator(
                    bigtableEmulator.getMappedPort(BIGTABLE_EMULATOR_PORT))
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build());
    adminClient =
        BigtableTableAdminClient.create(
            BigtableTableAdminSettings.newBuilderForEmulator(
                    bigtableEmulator.getMappedPort(BIGTABLE_EMULATOR_PORT))
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build());
    Configuration config = BigtableConfiguration.configure(PROJECT_ID, INSTANCE_ID);
    config.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + bigtableEmulator.getMappedPort(BIGTABLE_EMULATOR_PORT));
    hbaseClient = BigtableConfiguration.connect(config);
    ingestData();
  }

  private static void ingestData() throws IOException {
    String featureTableName = "rides";

    /** Single Entity Ingestion Workflow */
    Schema schema =
        SchemaBuilder.record("DriverData")
            .namespace(featureTableName)
            .fields()
            .requiredLong("trip_cost")
            .requiredDouble("trip_distance")
            .nullableString("trip_empty", "null")
            .requiredString("trip_wrong_type")
            .endRecord();
    createTable(FEAST_PROJECT, List.of("driver"), List.of(featureTableName));
    insertSchema(FEAST_PROJECT, List.of("driver"), schema);

    GenericRecord record =
        new GenericRecordBuilder(schema)
            .set("trip_cost", 5L)
            .set("trip_distance", 3.5)
            .set("trip_empty", null)
            .set("trip_wrong_type", "test")
            .build();
    String entityKey = String.valueOf(DataGenerator.createInt64Value(1).getInt64Val());
    insertRow(FEAST_PROJECT, List.of("driver"), entityKey, featureTableName, schema, record);
  }

  private static String getTableName(String project, List<String> entityNames) {
    return String.format("%s__%s", project, String.join("__", entityNames));
  }

  private static void createTable(
      String project, List<String> entityNames, List<String> featureTables) {
    String tableName = getTableName(project, entityNames);
    CreateTableRequest createTableRequest = CreateTableRequest.of(tableName);
    List<String> columnFamilies =
        Stream.concat(featureTables.stream(), Stream.of("metadata")).toList();
    for (String columnFamily : columnFamilies) {
      createTableRequest.addFamily(columnFamily);
    }
    if (!adminClient.exists(tableName)) {
      adminClient.createTable(createTableRequest);
    }
  }

  private static byte[] serializedSchemaReference(Schema schema) {
    return Hashing.murmur3_32().hashBytes(schema.toString().getBytes()).asBytes();
  }

  private static void insertSchema(String project, List<String> entityNames, Schema schema)
      throws IOException {
    String tableName = getTableName(project, entityNames);
    byte[] schemaReference = serializedSchemaReference(schema);
    byte[] schemaKey = createSchemaKey(schemaReference);
    client.mutateRow(
        RowMutation.create(tableName, ByteString.copyFrom(schemaKey))
            .setCell(
                "metadata",
                ByteString.copyFrom("avro".getBytes()),
                ByteString.copyFrom(schema.toString().getBytes())));
  }

  private static byte[] createSchemaKey(byte[] schemaReference) throws IOException {
    String schemaKeyPrefix = "schema#";

    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(schemaKeyPrefix.getBytes());
    concatOutputStream.write(schemaReference);
    return concatOutputStream.toByteArray();
  }

  private static byte[] createEntityValue(Schema schema, GenericRecord record) throws IOException {
    byte[] schemaReference = serializedSchemaReference(schema);
    // Entity-Feature Row
    byte[] avroSerializedFeatures = recordToAvro(record, schema);

    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(schemaReference);
    concatOutputStream.write("".getBytes());
    concatOutputStream.write(avroSerializedFeatures);
    byte[] entityFeatureValue = concatOutputStream.toByteArray();

    return entityFeatureValue;
  }

  private static byte[] recordToAvro(GenericRecord datum, Schema schema) throws IOException {
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(datum, encoder);
    encoder.flush();

    return output.toByteArray();
  }

  private static void insertRow(
      String project,
      List<String> entityNames,
      String entityKey,
      String featureTableName,
      Schema schema,
      GenericRecord record)
      throws IOException {
    byte[] entityFeatureValue = createEntityValue(schema, record);
    String tableName = getTableName(project, entityNames);

    // Update Compound Entity-Feature Row
    client.mutateRow(
        RowMutation.create(tableName, ByteString.copyFrom(entityKey.getBytes()))
            .setCell(
                featureTableName,
                ByteString.copyFrom("".getBytes()),
                ByteString.copyFrom(entityFeatureValue)));
  }

  @Test
  public void shouldRetrieveFeaturesSuccessfully() {
    BigTableOnlineRetriever retriever = new BigTableOnlineRetriever(client);
    List<FeatureReference> featureReferences =
        Stream.of("trip_cost", "trip_distance")
            .map(f -> FeatureReference.newBuilder().setFeatureTable("rides").setName(f).build())
            .toList();
    List<String> entityNames = List.of("driver");
    List<EntityRow> entityRows =
        List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> featuresForRows =
        retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, featuresForRows.size());
    List<Feature> features = featuresForRows.get(0);
    assertEquals(2, features.size());
    assertEquals(5L, features.get(0).getFeatureValue(ValueType.Enum.INT64).getInt64Val());
    assertEquals(featureReferences.get(0), features.get(0).getFeatureReference());
    assertEquals(3.5, features.get(1).getFeatureValue(ValueType.Enum.DOUBLE).getDoubleVal());
    assertEquals(featureReferences.get(1), features.get(1).getFeatureReference());
  }

  @Test
  public void shouldFilterOutMissingFeatureRef() {
    BigTableOnlineRetriever retriever = new BigTableOnlineRetriever(client);
    List<FeatureReference> featureReferences =
        List.of(
            FeatureReference.newBuilder().setFeatureTable("rides").setName("not_exists").build());
    List<String> entityNames = List.of("driver");
    List<EntityRow> entityRows =
        List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> features =
        retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, features.size());
    assertEquals(0, features.get(0).size());
  }

  @Test
  public void shouldRetrieveFeaturesSuccessfullyWhenUsingHbase(){
    HBaseOnlineRetriever retriever = new HBaseOnlineRetriever(hbaseClient);
    List<FeatureReference> featureReferences =
            Stream.of("trip_cost", "trip_distance")
                    .map(f -> FeatureReference.newBuilder().setFeatureTable("rides").setName(f).build())
                    .toList();
    List<String> entityNames = List.of("driver");
    List<EntityRow> entityRows =
            List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> featuresForRows =
            retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, featuresForRows.size());
    List<Feature> features = featuresForRows.get(0);
    assertEquals(2, features.size());
    assertEquals(5L, features.get(0).getFeatureValue(ValueType.Enum.INT64).getInt64Val());
    assertEquals(featureReferences.get(0), features.get(0).getFeatureReference());
    assertEquals(3.5, features.get(1).getFeatureValue(ValueType.Enum.DOUBLE).getDoubleVal());
    assertEquals(featureReferences.get(1), features.get(1).getFeatureReference());

  }

  @Test
  public void shouldFilterOutMissingFeatureRefUsingHbase() {
    BigTableOnlineRetriever retriever = new BigTableOnlineRetriever(client);
    List<FeatureReference> featureReferences =
            List.of(
                    FeatureReference.newBuilder().setFeatureTable("rides").setName("not_exists").build());
    List<String> entityNames = List.of("driver");
    List<EntityRow> entityRows =
            List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> features =
            retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, features.size());
    assertEquals(0, features.get(0).size());
  }
}
