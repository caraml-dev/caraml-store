package dev.caraml.store.kubernetes.sparkapplication;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import lombok.Data;

@Data
public class SparkApplication implements KubernetesObject {

  private V1ObjectMeta metadata;
  private String apiVersion;
  private String kind = "SparkApplication";
  private SparkApplicationSpec spec;
}
