package dev.caraml.serving.store.bigtable;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

public class BigTableSchemaRegistry extends BaseSchemaRegistry {
  private final BigtableDataClient client;

  public BigTableSchemaRegistry(BigtableDataClient client) {
    this.client = client;

    CacheLoader<SchemaReference, GenericDatumReader<GenericRecord>> schemaCacheLoader =
        CacheLoader.from(this::loadReader);

    cache = CacheBuilder.newBuilder().build(schemaCacheLoader);
  }

  @Override
  public GenericDatumReader<GenericRecord> loadReader(SchemaReference reference) {
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
