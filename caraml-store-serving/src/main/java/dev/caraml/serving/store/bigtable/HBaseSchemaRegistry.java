package dev.caraml.serving.store.bigtable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class HBaseSchemaRegistry {
  private final Connection hbaseClient;
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

  public HBaseSchemaRegistry(Connection hbaseClient) {
    this.hbaseClient = hbaseClient;

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
    try {
      Table table = this.hbaseClient.getTable(TableName.valueOf(reference.getTableName()));

      byte[] rowKey =
          ByteString.copyFrom(KEY_PREFIX.getBytes())
              .concat(reference.getSchemaHash())
              .toByteArray();
      Get query = new Get(rowKey);
      query.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes());

      Result result = table.get(query);

      Cell last = result.getColumnLatestCell(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes());
      if (last == null) {
        // NOTE: this should never happen
        throw new RuntimeException("Schema not found");
      }
      ByteBuffer schemaBuffer =
          ByteBuffer.wrap(last.getValueArray())
              .position(last.getValueOffset())
              .limit(last.getValueOffset() + last.getValueLength())
              .slice();
      Schema schema = new Schema.Parser().parse(ByteString.copyFrom(schemaBuffer).toStringUtf8());
      return new GenericDatumReader<>(schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
