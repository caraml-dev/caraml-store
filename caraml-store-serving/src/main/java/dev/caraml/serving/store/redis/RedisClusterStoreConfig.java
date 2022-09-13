package dev.caraml.serving.store.redis;

import dev.caraml.serving.store.OnlineRetriever;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.NettyCustomizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollChannelOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.store.redis-cluster")
@ConditionalOnProperty("caraml.store.redis-cluster.enabled")
@Getter
@Setter
public class RedisClusterStoreConfig {
  private String connectionString;
  private String password;
  private ReadFrom readFrom;
  private Duration timeout;
  private TCPConfig tcpConfig;
  private TopologyRefreshConfig topologyRefreshConfig = TopologyRefreshConfig.DEFAULT;

  private static ClusterTopologyRefreshOptions getTopologyRefreshOptions(
      TopologyRefreshConfig topologyRefreshConfig) {
    ClusterTopologyRefreshOptions.Builder builder = ClusterTopologyRefreshOptions.builder();

    if (topologyRefreshConfig.isEnablePeriodicRefresh()) {
      builder =
          builder
              .enablePeriodicRefresh(topologyRefreshConfig.isEnablePeriodicRefresh())
              .refreshPeriod(Duration.ofSeconds(topologyRefreshConfig.getRefreshPeriodSecond()));
    }

    if (topologyRefreshConfig.isEnableAllAdaptiveTriggerRefresh()) {
      builder = builder.enableAllAdaptiveRefreshTriggers();
    }

    return builder.build();
  }

  @Bean
  public OnlineRetriever getRetriever() {
    List<RedisURI> redisURIList =
        Arrays.stream(connectionString.split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  RedisURI.Builder uriBuilder =
                      RedisURI.builder()
                          .withHost(hostPortSplit[0])
                          .withPort(Integer.parseInt(hostPortSplit[1]));
                  if (!StringUtils.isEmpty(password)) {
                    uriBuilder.withPassword(password.toCharArray());
                  }
                  return uriBuilder.build();
                })
            .collect(Collectors.toList());

    io.lettuce.core.cluster.RedisClusterClient client =
        tcpConfig == null
            ? io.lettuce.core.cluster.RedisClusterClient.create(redisURIList)
            : io.lettuce.core.cluster.RedisClusterClient.create(
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
                redisURIList);
    client.setOptions(
        ClusterClientOptions.builder()
            .socketOptions(SocketOptions.builder().keepAlive(true).tcpNoDelay(true).build())
            .timeoutOptions(TimeoutOptions.enabled(timeout))
            .pingBeforeActivateConnection(true)
            .topologyRefreshOptions(getTopologyRefreshOptions(topologyRefreshConfig))
            .build());

    StatefulRedisClusterConnection<byte[], byte[]> connection =
        client.connect(new ByteArrayCodec());
    connection.setReadFrom(readFrom);

    RedisClientAdapter adapter = new RedisClusterClient(connection);
    return new RedisOnlineRetriever(adapter);
  }
}
