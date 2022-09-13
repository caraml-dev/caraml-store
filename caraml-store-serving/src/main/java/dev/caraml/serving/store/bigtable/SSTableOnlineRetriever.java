package dev.caraml.serving.store.bigtable;

import com.google.common.hash.Hashing;
import dev.caraml.serving.store.Feature;
import dev.caraml.serving.store.OnlineRetriever;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import dev.caraml.store.protobuf.types.ValueProto;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @param <K> Decoded value type of the partition key
 * @param <V> Type of the SSTable row
 */
public interface SSTableOnlineRetriever<K, V> extends OnlineRetriever {

  int MAX_TABLE_NAME_LENGTH = 50;

  @Override
  default List<List<Feature>> getOnlineFeatures(
      String project,
      List<EntityRow> entityRows,
      List<FeatureReference> featureReferences,
      List<String> entityNames) {

    List<String> columnFamilies = getSSTableColumns(featureReferences);
    String tableName = getSSTable(project, entityNames);

    if (!validateEntityRowsConsistency(entityRows, entityNames)) {
      throw new RuntimeException(
          "Entity rows are inconsistent. The same entities must be present in all rows");
    }

    List<K> rowKeys =
        entityRows.stream()
            .map(row -> convertEntityValueToKey(row, entityNames))
            .collect(Collectors.toList());

    Map<K, V> rowsFromSSTable = getFeaturesFromSSTable(tableName, rowKeys, columnFamilies);

    return convertRowToFeature(tableName, rowKeys, rowsFromSSTable, featureReferences);
  }

  default boolean validateEntityRowsConsistency(
      List<EntityRow> entityRows, List<String> entityNames) {
    return entityRows.stream()
        .allMatch(row -> row.getFieldsMap().keySet().containsAll(entityNames));
  }

  /**
   * Generate SSTable key.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return SSTable key for retrieval
   */
  K convertEntityValueToKey(EntityRow entityRow, List<String> entityNames);

  /**
   * Converts SSTable rows into @NativeFeature type.
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  List<List<Feature>> convertRowToFeature(
      String tableName, List<K> rowKeys, Map<K, V> rows, List<FeatureReference> featureReferences);

  /**
   * Retrieve rows for each row entity key.
   *
   * @param tableName Name of SSTable
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of column names
   * @return Map of retrieved features for each rowKey
   */
  Map<K, V> getFeaturesFromSSTable(String tableName, List<K> rowKeys, List<String> columnFamilies);

  /**
   * Retrieve name of SSTable corresponding to entities in retrieval call
   *
   * @param project Name of Feast project
   * @param entityNames List of entities used in retrieval call
   * @return Name of Cassandra table
   */
  default String getSSTable(String project, List<String> entityNames) {
    String sortedJoinedEntityNames =
        entityNames.stream().sorted().collect(Collectors.joining("__"));
    return trimAndHash(
        String.format("%s__%s", project, sortedJoinedEntityNames), MAX_TABLE_NAME_LENGTH);
  }

  /**
   * Convert Entity value from Feast valueType to String type. Currently only supports STRING_VAL,
   * INT64_VAL, INT32_VAL and BYTES_VAL.
   *
   * @param v Entity value of Feast valueType
   * @return String representation of Entity value
   */
  default String valueToString(ValueProto.Value v) {
    String stringRepr;
    switch (v.getValCase()) {
      case STRING_VAL:
        stringRepr = v.getStringVal();
        break;
      case INT64_VAL:
        stringRepr = String.valueOf(v.getInt64Val());
        break;
      case INT32_VAL:
        stringRepr = String.valueOf(v.getInt32Val());
        break;
      case BYTES_VAL:
        stringRepr = v.getBytesVal().toString();
        break;
      default:
        throw new RuntimeException("Type is not supported to be entity");
    }

    return stringRepr;
  }

  /**
   * Retrieve SSTable columns based on Feature references.
   *
   * @param featureReferences List of feature references in retrieval call
   * @return List of String of column names
   */
  default List<String> getSSTableColumns(List<FeatureReference> featureReferences) {
    return featureReferences.stream()
        .map(FeatureReference::getFeatureTable)
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * Trims long SSTable table names and appends hash suffix for uniqueness.
   *
   * @param expr Original SSTable table name
   * @param maxLength Maximum length allowed for SSTable
   * @return Hashed suffix SSTable table name
   */
  default String trimAndHash(String expr, int maxLength) {
    // Length 8 as derived from murmurhash_32 implementation
    int maxPrefixLength = maxLength - 8;
    String finalName = expr;
    if (expr.length() > maxLength) {
      String hashSuffix =
          Hashing.murmur3_32().hashBytes(expr.substring(maxPrefixLength).getBytes()).toString();
      finalName = expr.substring(0, Math.min(expr.length(), maxPrefixLength)).concat(hashSuffix);
    }
    return finalName;
  }
}
