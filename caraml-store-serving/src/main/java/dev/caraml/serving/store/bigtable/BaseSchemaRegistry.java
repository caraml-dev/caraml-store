package dev.caraml.serving.store.bigtable;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public abstract class BaseSchemaRegistry {
  protected LoadingCache<SchemaReference, GenericDatumReader<GenericRecord>> cache = null;

  protected static String COLUMN_FAMILY = "metadata";
  protected static String QUALIFIER = "avro";
  protected static String KEY_PREFIX = "schema#";

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

  public GenericDatumReader<GenericRecord> getReader(SchemaReference reference) {
    GenericDatumReader<GenericRecord> reader;
    try {
      reader = this.cache.get(reference);
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new RuntimeException(String.format("Unable to find Schema"), e);
    }
    return reader;
  }

  public abstract GenericDatumReader<GenericRecord> loadReader(SchemaReference reference);
}
