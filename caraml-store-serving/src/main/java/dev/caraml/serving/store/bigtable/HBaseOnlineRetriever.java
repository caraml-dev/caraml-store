package dev.caraml.serving.store.bigtable;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import dev.caraml.serving.store.AvroFeature;
import dev.caraml.serving.store.Feature;
import dev.caraml.store.protobuf.serving.ServingServiceProto;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class HBaseOnlineRetriever implements SSTableOnlineRetriever<ByteString, Result> {
  private final Connection client;
  private final HBaseSchemaRegistry schemaRegistry;

  public HBaseOnlineRetriever(Connection client) {
    this.client = client;
    this.schemaRegistry = new HBaseSchemaRegistry(client);
  }

  /**
   * Generate Hbase key in the form of entity values joined by #.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return
   */
  @Override
  public ByteString convertEntityValueToKey(
      ServingServiceProto.GetOnlineFeaturesRequest.EntityRow entityRow, List<String> entityNames) {
    return ByteString.copyFrom(
        entityNames.stream()
            .sorted()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  /**
   * Converts rowCell feature into @NativeFeature type, HBase specific implementation
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  @Override
  public List<List<Feature>> convertRowToFeature(
      String tableName,
      List<ByteString> rowKeys,
      Map<ByteString, Result> rows,
      List<ServingServiceProto.FeatureReference> featureReferences) {
    BinaryDecoder reusedDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

    return rowKeys.stream()
        .map(
            rowKey -> {
              if (!rows.containsKey(rowKey)) {
                return Collections.<Feature>emptyList();
              } else {
                Result row = rows.get(rowKey);
                return featureReferences.stream()
                    .map(ServingServiceProto.FeatureReference::getFeatureTable)
                    .distinct()
                    .map(cf -> row.getColumnCells(cf.getBytes(), null))
                    .filter(ls -> !ls.isEmpty())
                    .flatMap(
                        rowCells -> {
                          Cell rowCell = rowCells.get(0); // Latest cell
                          ByteBuffer valueBuffer =
                              HBaseSchemaRegistry.GetValueByteBufferFromRowCell(rowCell);
                          ByteBuffer familyBuffer =
                              ByteBuffer.wrap(rowCell.getFamilyArray())
                                  .position(rowCell.getFamilyOffset())
                                  .limit(rowCell.getFamilyOffset() + rowCell.getFamilyLength())
                                  .slice();
                          String family = ByteString.copyFrom(familyBuffer).toStringUtf8();
                          ByteString value = ByteString.copyFrom(valueBuffer);

                          List<Feature> features;
                          List<ServingServiceProto.FeatureReference> localFeatureReferences =
                              featureReferences.stream()
                                  .filter(
                                      featureReference ->
                                          featureReference.getFeatureTable().equals(family))
                                  .collect(Collectors.toList());

                          try {
                            features =
                                decodeFeatures(
                                    tableName,
                                    value,
                                    localFeatureReferences,
                                    reusedDecoder,
                                    rowCell.getTimestamp());
                          } catch (IOException e) {
                            throw new RuntimeException("Failed to decode features from BigTable");
                          }

                          return features.stream();
                        })
                    .collect(Collectors.toList());
              }
            })
        .collect(Collectors.toList());
  }

  /**
   * Retrieve rows with required column families for each row entity by sending batch Get request,
   * HBase specific implementation
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of column names
   * @return
   */
  @Override
  public Map<ByteString, Result> getFeaturesFromSSTable(
      String tableName, List<ByteString> rowKeys, List<String> columnFamilies) {
    try {
      Table table = this.client.getTable(TableName.valueOf(tableName));

      // construct query get list
      List<Get> queryGetList = new ArrayList<>();
      rowKeys.forEach(
          rowKey -> {
            Get get = new Get(rowKey.toByteArray());
            columnFamilies.forEach(cf -> get.addFamily(cf.getBytes()));

            queryGetList.add(get);
          });

      // fetch data from table
      Result[] rows = table.get(queryGetList);

      // construct result
      Map<ByteString, Result> result = new HashMap<>();
      Arrays.stream(rows)
          .filter(row -> !row.isEmpty())
          .forEach(row -> result.put(ByteString.copyFrom(row.getRow()), row));

      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Decode features from Avro serialized bytes
   *
   * @param tableName Name of Hbase table
   * @param value Value of HBase cell where first 4 bytes represents the schema reference and the
   *     remaining bytes represent the avro-serialized features
   * @param featureReferences List of feature references
   * @param reusedDecoder Decoder for decoding feature values
   * @param timestamp Timesttamp of rowcell
   * @return @NativeFeature with retrieved value stored in Hbase Cell
   * @throws IOException
   */
  private List<Feature> decodeFeatures(
      String tableName,
      ByteString value,
      List<ServingServiceProto.FeatureReference> featureReferences,
      BinaryDecoder reusedDecoder,
      long timestamp)
      throws IOException {
    ByteString schemaReferenceBytes =
        value.substring(0, HBaseSchemaRegistry.SCHEMA_REFERENCE_LENGTH);
    byte[] featureValueBytes =
        value.substring(HBaseSchemaRegistry.SCHEMA_REFERENCE_LENGTH).toByteArray();

    HBaseSchemaRegistry.SchemaReference schemaReference =
        new HBaseSchemaRegistry.SchemaReference(tableName, schemaReferenceBytes);

    GenericDatumReader<GenericRecord> reader = this.schemaRegistry.getReader(schemaReference);

    reusedDecoder = DecoderFactory.get().binaryDecoder(featureValueBytes, reusedDecoder);
    GenericRecord record = reader.read(null, reusedDecoder);

    return featureReferences.stream()
        .map(
            featureReference -> {
              Object featureValue;
              try {
                featureValue = record.get(featureReference.getName());
              } catch (AvroRuntimeException e) {
                // Feature is not found in schema
                return null;
              }
              return new AvroFeature(
                  featureReference,
                  Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                  Objects.requireNonNullElseGet(featureValue, Object::new));
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
