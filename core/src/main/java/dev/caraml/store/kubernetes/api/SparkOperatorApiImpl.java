package dev.caraml.store.kubernetes.api;

import dev.caraml.store.config.ClusterConfig;
import dev.caraml.store.kubernetes.sparkapplication.SparkApplication;
import dev.caraml.store.kubernetes.sparkapplication.SparkApplicationList;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkOperatorApiImpl implements SparkOperatorApi {

  private final GenericKubernetesApi<SparkApplication, SparkApplicationList> api;

  @Autowired
  public SparkOperatorApiImpl(ClusterConfig cluster) throws IOException {
    ApiClient client =
        cluster.inCluster() ? ClientBuilder.cluster().build() : Config.defaultClient();
    Configuration.setDefaultApiClient(client);
    this.api =
        new GenericKubernetesApi<>(
            SparkApplication.class,
            SparkApplicationList.class,
            "sparkoperator.k8s.io",
            "v1beta2",
            "sparkapplications",
            client);
  }

  @Override
  public SparkApplication update(SparkApplication app) throws SparkOperatorApiException {
    try {
      return api.update(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public SparkApplication create(SparkApplication app) throws SparkOperatorApiException {
    try {
      return api.create(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public List<SparkApplication> list(String namespace, String labelSelector)
      throws SparkOperatorApiException {
    ListOptions options = new ListOptions();
    options.setLabelSelector(labelSelector);
    try {
      return api.list(namespace, options).throwsApiException().getObject().getItems();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public Optional<SparkApplication> get(String namespace, String name)
      throws SparkOperatorApiException {
    KubernetesApiResponse<SparkApplication> resp = api.get(namespace, name);
    return switch (resp.getHttpStatusCode()) {
      case 200, 404 -> Optional.ofNullable(resp.getObject());
      default -> throw new SparkOperatorApiException(resp.getStatus().toString());
    };
  }
}
