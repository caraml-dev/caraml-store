package dev.caraml.store.kubernetes.sparkapplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SparkApplicationSpec {
  private DynamicAllocation dynamicAllocation;
  private SparkDriverSpec driver;
  private SparkExecutorSpec executor;
  private List<String> arguments = new ArrayList<>();
  private Map<String, String> hadoopConf;
  private Map<String, String> sparkConf;
  private String image;
  private String mode = "cluster";
  private Integer timeToLiveSeconds;
  private RestartPolicy restartPolicy;
  private String type;

  public void addArgument(String name, String value) {
    arguments.add("--" + name);
    arguments.add(value);
  }
}
