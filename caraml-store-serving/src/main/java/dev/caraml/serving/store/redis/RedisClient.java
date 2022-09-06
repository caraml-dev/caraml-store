package dev.caraml.serving.store.redis;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.NettyCustomizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollChannelOption;
import java.util.List;

public class RedisClient implements RedisClientAdapter {

  private final RedisAsyncCommands<byte[], byte[]> asyncCommands;

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }

  public RedisClient(StatefulRedisConnection<byte[], byte[]> connection) {
    this.asyncCommands = connection.async();

    // Disable auto-flushing
    // this.asyncCommands.setAutoFlushCommands(false);
  }

  public static RedisClientAdapter create(RedisStoreConfig config) {
    RedisURI.Builder uriBuilder =
        RedisURI.builder().withHost(config.getHost()).withPort(config.getPort());
    if (!config.getPassword().isEmpty()) {
      uriBuilder.withPassword(config.getPassword().toCharArray());
    }
    if (config.getSsl()) {
      uriBuilder.withSsl(config.getSsl());
    }
    RedisURI uri = uriBuilder.build();

    TCPConfig tcpConfig = config.getTcpConfig();
    io.lettuce.core.RedisClient client =
        tcpConfig == null
            ? io.lettuce.core.RedisClient.create(uri)
            : io.lettuce.core.RedisClient.create(
                ClientResources.builder()
                    .nettyCustomizer(
                        new NettyCustomizer() {
                          @Override
                          public void afterBootstrapInitialized(Bootstrap bootstrap) {
                            bootstrap.option(
                                EpollChannelOption.TCP_KEEPIDLE, tcpConfig.getKeepIdle());
                            bootstrap.option(
                                EpollChannelOption.TCP_KEEPINTVL, tcpConfig.getKeepInterval());
                            bootstrap.option(
                                EpollChannelOption.TCP_KEEPCNT, tcpConfig.getKeepConnection());
                            bootstrap.option(
                                EpollChannelOption.TCP_USER_TIMEOUT, tcpConfig.getUserTimeout());
                          }
                        })
                    .build(),
                uri);

    StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());

    return new RedisClient(connection);
  }
}
