package dev.caraml.serving.store.bigtable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class HBaseSchemaRegistry extends BaseSchemaRegistry {
  private final Connection hbaseClient;

  public HBaseSchemaRegistry(Connection hbaseClient) {
    this.hbaseClient = hbaseClient;

    CacheLoader<SchemaReference, GenericDatumReader<GenericRecord>> schemaCacheLoader =
        CacheLoader.from(this::loadReader);

    cache = CacheBuilder.newBuilder().build(schemaCacheLoader);
  }

  @Override
  public GenericDatumReader<GenericRecord> loadReader(SchemaReference reference) {
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
