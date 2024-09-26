package dev.caraml.serving.store.bigtable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.hash.Hashing;
import dev.caraml.serving.store.Feature;
import dev.caraml.store.protobuf.serving.ServingServiceProto;
import dev.caraml.store.protobuf.types.ValueProto;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class HbaseOnlineRetrieverTest {
  static Connection hbaseClient;
  static HBaseAdmin admin;
  static Configuration hbaseConfiguration = HBaseConfiguration.create();
  static final String FEAST_PROJECT = "default";

  @Container public static GenericHbase2Container hbase = new GenericHbase2Container();

  @BeforeAll
  public static void setup() throws IOException {
    //    hbaseConfiguration.set("hbase.zookeeper.quorum", hbase.getHost());
    //    hbaseConfiguration.set("hbase.zookeeper.property.clientPort",
    // hbase.getMappedPort(2181).toString());
    //    hbaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
    //    hbaseClient = ConnectionFactory.createConnection(hbaseConfiguration);
    hbaseClient = ConnectionFactory.createConnection(hbase.hbase2Configuration);
    admin = (HBaseAdmin) hbaseClient.getAdmin();
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

  private static byte[] serializedSchemaReference(Schema schema) {
    return Hashing.murmur3_32().hashBytes(schema.toString().getBytes()).asBytes();
  }

  private static void createTable(
      String project, List<String> entityNames, List<String> featureTables) {
    String tableName = getTableName(project, entityNames);

    List<String> columnFamilies =
        Stream.concat(featureTables.stream(), Stream.of("metadata")).toList();
    TableDescriptorBuilder tb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
    columnFamilies.forEach(cf -> tb.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf)));
    try {
      if (admin.tableExists(TableName.valueOf(tableName))) {
        return;
      }
      admin.createTable(tb.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void insertSchema(String project, List<String> entityNames, Schema schema)
      throws IOException {
    String tableName = getTableName(project, entityNames);
    byte[] schemaReference = serializedSchemaReference(schema);
    byte[] schemaKey = createSchemaKey(schemaReference);
    Table table = hbaseClient.getTable(TableName.valueOf(tableName));
    Put put = new Put(schemaKey);
    put.addColumn("metadata".getBytes(), "avro".getBytes(), schema.toString().getBytes());
    table.put(put);
    table.close();
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
    Table table = hbaseClient.getTable(TableName.valueOf(tableName));
    Put put = new Put(entityKey.getBytes());
    put.addColumn(featureTableName.getBytes(), "".getBytes(), entityFeatureValue);
    table.put(put);
    table.close();
  }

  @Test
  public void shouldRetrieveFeaturesSuccessfully() {
    HBaseOnlineRetriever retriever = new HBaseOnlineRetriever(hbaseClient);
    List<ServingServiceProto.FeatureReference> featureReferences =
        Stream.of("trip_cost", "trip_distance")
            .map(
                f ->
                    ServingServiceProto.FeatureReference.newBuilder()
                        .setFeatureTable("rides")
                        .setName(f)
                        .build())
            .toList();
    List<String> entityNames = List.of("driver");
    List<ServingServiceProto.GetOnlineFeaturesRequest.EntityRow> entityRows =
        List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> featuresForRows =
        retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, featuresForRows.size());
    List<Feature> features = featuresForRows.get(0);
    assertEquals(2, features.size());
    assertEquals(
        5L, features.get(0).getFeatureValue(ValueProto.ValueType.Enum.INT64).getInt64Val());
    assertEquals(featureReferences.get(0), features.get(0).getFeatureReference());
    assertEquals(
        3.5, features.get(1).getFeatureValue(ValueProto.ValueType.Enum.DOUBLE).getDoubleVal());
    assertEquals(featureReferences.get(1), features.get(1).getFeatureReference());
  }

  @Test
  public void shouldFilterOutMissingFeatureRefUsingHbase() {
    HBaseOnlineRetriever retriever = new HBaseOnlineRetriever(hbaseClient);
    List<ServingServiceProto.FeatureReference> featureReferences =
        List.of(
            ServingServiceProto.FeatureReference.newBuilder()
                .setFeatureTable("rides")
                .setName("not_exists")
                .build());
    List<String> entityNames = List.of("driver");
    List<ServingServiceProto.GetOnlineFeaturesRequest.EntityRow> entityRows =
        List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> features =
        retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, features.size());
    assertEquals(0, features.get(0).size());
  }
}
