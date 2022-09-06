package dev.caraml.serving.store.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public class BigTableSchemaRegistry {
  private final BigtableDataClient client;
  private final LoadingCache<SchemaReference, GenericDatumReader<GenericRecord>> cache;

  private static String COLUMN_FAMILY = "metadata";
  private static String QUALIFIER = "avro";
  private static String KEY_PREFIX = "schema#";

  public static class SchemaReference {
    private final String tableName;
    private final ByteString schemaHash;

    public SchemaReference(String tableName, ByteString schemaHash) {
      this.tableName = tableName;
      this.schemaHash = schemaHash;
    }

    public String getTableName() {
      return tableName;
    }

    public ByteString getSchemaHash() {
      return schemaHash;
    }

    @Override
    public int hashCode() {
      int result = tableName.hashCode();
      result = 31 * result + schemaHash.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SchemaReference that = (SchemaReference) o;

      if (!tableName.equals(that.tableName)) return false;
      return schemaHash.equals(that.schemaHash);
    }
  }

  public BigTableSchemaRegistry(BigtableDataClient client) {
    this.client = client;

    CacheLoader<SchemaReference, GenericDatumReader<GenericRecord>> schemaCacheLoader =
        CacheLoader.from(this::loadReader);

    cache = CacheBuilder.newBuilder().build(schemaCacheLoader);
  }

  public GenericDatumReader<GenericRecord> getReader(SchemaReference reference) {
    GenericDatumReader<GenericRecord> reader;
    try {
      reader = this.cache.get(reference);
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new RuntimeException(String.format("Unable to find Schema"), e);
    }
    return reader;
  }

  private GenericDatumReader<GenericRecord> loadReader(SchemaReference reference) {
    Row row =
        client.readRow(
            reference.getTableName(),
            ByteString.copyFrom(KEY_PREFIX.getBytes()).concat(reference.getSchemaHash()),
            Filters.FILTERS.family().exactMatch(COLUMN_FAMILY));
    RowCell last = Iterables.getLast(row.getCells(COLUMN_FAMILY, QUALIFIER));

    Schema schema = new Schema.Parser().parse(last.getValue().toStringUtf8());
    return new GenericDatumReader<>(schema);
  }
}
