package dev.caraml.serving.store.redis;

import dev.caraml.serving.store.OnlineRetriever;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.NettyCustomizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollChannelOption;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.store.redis")
@ConditionalOnProperty(prefix = "caraml.store", name = "active", havingValue = "redis")
@Getter
@Setter
public class RedisStoreConfig {
  private String host;
  private Integer port;
  private String password;
  private Boolean ssl;
  private TCPConfig tcp;

  @Bean
  public OnlineRetriever getRetriever() {
    RedisURI.Builder uriBuilder = RedisURI.builder().withHost(host).withPort(port);
    if (!password.isEmpty()) {
      uriBuilder.withPassword(password.toCharArray());
    }
    if (ssl) {
      uriBuilder.withSsl(ssl);
    }
    RedisURI uri = uriBuilder.build();

    io.lettuce.core.RedisClient client =
        tcp == null
            ? io.lettuce.core.RedisClient.create(uri)
            : io.lettuce.core.RedisClient.create(
                ClientResources.builder()
                    .nettyCustomizer(
                        new NettyCustomizer() {
                          @Override
                          public void afterBootstrapInitialized(Bootstrap bootstrap) {
                            bootstrap.option(EpollChannelOption.TCP_KEEPIDLE, tcp.getKeepIdle());
                            bootstrap.option(
                                EpollChannelOption.TCP_KEEPINTVL, tcp.getKeepInterval());
                            bootstrap.option(
                                EpollChannelOption.TCP_KEEPCNT, tcp.getKeepConnection());
                            bootstrap.option(
                                EpollChannelOption.TCP_USER_TIMEOUT, tcp.getUserTimeout());
                          }
                        })
                    .build(),
                uri);

    StatefulRedisConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
    RedisClientAdapter adapter = new RedisClient(connection);
    return new RedisOnlineRetriever(adapter);
  }
}
