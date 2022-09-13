package dev.caraml.store.sparkjob.crd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
  private List<String> imagePullSecrets;

  public void addArguments(List<String> newArguments) {
    arguments.addAll(newArguments);
  }

  public SparkApplicationSpec deepCopy() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(mapper.writeValueAsString(this), SparkApplicationSpec.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
