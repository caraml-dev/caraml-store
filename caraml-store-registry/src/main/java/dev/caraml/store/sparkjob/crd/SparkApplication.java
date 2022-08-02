package dev.caraml.store.sparkjob.crd;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class SparkApplication implements KubernetesObject {

  private V1ObjectMeta metadata;
  private String apiVersion;
  private String kind = "SparkApplication";
  private SparkApplicationSpec spec;

  public void addLabels(Map<String, String> labels) {
    if (metadata.getLabels() == null) {
      metadata.setLabels(new HashMap<>());
    }
    labels.forEach((labelName, labelValue) -> metadata.getLabels().put(labelName, labelValue));
  }
}
