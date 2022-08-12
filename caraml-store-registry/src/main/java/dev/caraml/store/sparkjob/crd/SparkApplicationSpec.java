package dev.caraml.store.sparkjob.crd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SparkApplicationSpec {
  private String mainApplicationFile;
  private String mainClass;
  private DynamicAllocation dynamicAllocation;
  private SparkDriverSpec driver;
  private SparkExecutorSpec executor;
  private List<String> arguments = new ArrayList<>();
  private Map<String, String> hadoopConf;
  private Map<String, String> sparkConf;
  private String image;
  private DeployMode mode = DeployMode.CLUSTER;
  private Integer timeToLiveSeconds;
  private RestartPolicy restartPolicy;
  private String type;
  private String sparkVersion;
  private String pythonVersion;

  public void addArguments(List<String> newArguments) {
    arguments.addAll(newArguments);
  }
}
