package dev.caraml.store.kubernetes.sparkapplication;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.openapi.models.V1ListMeta;
import java.util.List;
import lombok.Data;

@Data
public class SparkApplicationList implements KubernetesListObject {

  private V1ListMeta metadata;
  private String apiVersion;
  private String kind;
  private List<SparkApplication> items;
}
