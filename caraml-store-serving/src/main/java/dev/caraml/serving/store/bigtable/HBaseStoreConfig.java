package dev.caraml.serving.store.bigtable;

import dev.caraml.serving.store.OnlineRetriever;
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "caraml.store.hbase")
@ConditionalOnProperty(prefix = "caraml.store", name = "active", havingValue = "hbase")
@Getter
@Setter
public class HBaseStoreConfig {
  private String zookeeperQuorum;
  private String zookeeperClientPort;

  @Bean
  public OnlineRetriever getRetriever() {
    org.apache.hadoop.conf.Configuration conf;
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
    conf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
    Connection connection;
    try {
      connection = ConnectionFactory.createConnection(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new HBaseOnlineRetriever(connection);
  }
}
