package dev.caraml.serving.store.bigtable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class GenericHbase2Container extends GenericContainer<GenericHbase2Container> {

  private final String hostName;
  public final Configuration hbase2Configuration = HBaseConfiguration.create();

  public GenericHbase2Container() {
    super(DockerImageName.parse("jcjabouille/hbase-standalone:2.4.9"));
    {
      try {
        hostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }
    int masterPort = 16010;
    addExposedPort(masterPort);
    int regionPort = 16011;
    addExposedPort(regionPort);
    addExposedPort(2181);

    withCreateContainerCmdModifier(
        cmd -> {
          cmd.withHostName(hostName);
        });

    waitingFor(Wait.forLogMessage(".*running regionserver.*", 1));
    withStartupTimeout(Duration.ofMinutes(10));

    withEnv("HBASE_MASTER_PORT", Integer.toString(masterPort));
    withEnv("HBASE_REGION_PORT", Integer.toString(regionPort));
//    setPortBindings(
//        Arrays.asList(
//            String.format("%d:%d", masterPort, masterPort),
//            String.format("%d:%d", regionPort, regionPort)));

    // Set network mode to host
    withNetworkMode("host");
  }

  @Override
  protected void doStart() {
    super.doStart();

    hbase2Configuration.set("hbase.client.pause", "200");
    hbase2Configuration.set("hbase.client.retries.number", "10");
    hbase2Configuration.set("hbase.rpc.timeout", "3000");
    hbase2Configuration.set("hbase.client.operation.timeout", "3000");
    hbase2Configuration.set("hbase.client.scanner.timeout.period", "10000");
    hbase2Configuration.set("zookeeper.session.timeout", "10000");
    hbase2Configuration.set("hbase.zookeeper.quorum", "localhost");
    hbase2Configuration.set(
        "hbase.zookeeper.property.clientPort", Integer.toString(getMappedPort(2181)));
  }
}
