package dev.caraml.serving.store.redis;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import java.util.List;

public class RedisClusterClient implements RedisClientAdapter {

  private final RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;

  public RedisClusterClient(StatefulRedisClusterConnection<byte[], byte[]> connection) {
    asyncCommands = connection.async();

    // allows reading from replicas
    asyncCommands.readOnly();

    // Disable auto-flushing
    // asyncCommands.setAutoFlushCommands(false);

  }

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }
}
