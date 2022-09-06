package dev.caraml.serving.store.redis;

import io.lettuce.core.*;
import java.util.List;

public interface RedisClientAdapter {
  RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields);

  void flushCommands();
}
