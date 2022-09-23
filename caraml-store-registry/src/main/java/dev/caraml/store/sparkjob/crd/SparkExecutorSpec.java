package dev.caraml.store.sparkjob.crd;

import io.kubernetes.client.openapi.models.V1Toleration;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SparkExecutorSpec {

  private Map<String, String> nodeSelector;
  private List<V1Toleration> tolerations;
  private Integer cores;
  private String coreRequest;
  private Integer instances;
  private String memory;
  private String javaOptions;
  private Map<String, String> labels;
  private Map<String, String> annotations;
  private List<SecretInfo> secrets;
}
