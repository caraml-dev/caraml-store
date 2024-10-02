package dev.caraml.serving.store.bigtable;

import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class GenericHbase2Container extends GenericContainer<GenericHbase2Container> {

  private final String hostName = "hbase-docker";
  public final Configuration hbase2Configuration = HBaseConfiguration.create();

  public GenericHbase2Container() {
    super(DockerImageName.parse("dajobe/hbase:latest"));
    withCreateContainerCmdModifier(
        cmd -> {
          cmd.withHostName(hostName);
        });

    withNetworkMode("host");
    withEnv("HBASE_DOCKER_HOSTNAME", "127.0.0.1");

    waitingFor(Wait.forLogMessage(".*master.HMaster: Master has completed initialization.*", 1));
    withStartupTimeout(Duration.ofMinutes(10));
  }

  @Override
  protected void doStart() {
    super.doStart();

    hbase2Configuration.set("hbase.client.pause", "200");
    hbase2Configuration.set("hbase.client.retries.number", "10");
    hbase2Configuration.set("hbase.rpc.timeout", "3000");
    hbase2Configuration.set("hbase.client.operation.timeout", "3000");
    hbase2Configuration.set("hbase.rpc.timeout", "3000");
    hbase2Configuration.set("hbase.client.scanner.timeout.period", "10000");
    hbase2Configuration.set("zookeeper.session.timeout", "10000");
    hbase2Configuration.set("hbase.zookeeper.quorum", "localhost");
    hbase2Configuration.set("hbase.zookeeper.property.clientPort", "2181");
  }
}
