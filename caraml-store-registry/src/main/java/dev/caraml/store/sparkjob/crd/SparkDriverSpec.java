package dev.caraml.store.sparkjob.crd;

import io.kubernetes.client.openapi.models.V1Affinity;
import io.kubernetes.client.openapi.models.V1Toleration;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SparkDriverSpec {

  private Map<String, String> nodeSelector;
  private V1Affinity affinity;
  private List<V1Toleration> tolerations;
  private Integer cores;
  private String coreRequest;
  private String mainApplicationFile;
  private String mainClass;
  private String memory;
  private String javaOptions;
  private Map<String, String> labels;
  private String serviceAccount;
  private Map<String, String> annotations;
  private Map<String, String> serviceAnnotations;
  private List<SecretInfo> secrets;
}
