package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.ScheduledSparkApplication;
import dev.caraml.store.sparkjob.crd.ScheduledSparkApplicationList;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import dev.caraml.store.sparkjob.crd.SparkApplicationList;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watchable;
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

  private final GenericKubernetesApi<SparkApplication, SparkApplicationList> sparkApplicationApi;
  private final GenericKubernetesApi<ScheduledSparkApplication, ScheduledSparkApplicationList>
      scheduledSparkApplicationApi;

  @Autowired
  public SparkOperatorApiImpl(ClusterConfig cluster) throws IOException {
    ApiClient client =
        cluster.getInCluster() ? ClientBuilder.cluster().build() : Config.defaultClient();
    Configuration.setDefaultApiClient(client);
    this.sparkApplicationApi =
        new GenericKubernetesApi<>(
            SparkApplication.class,
            SparkApplicationList.class,
            "sparkoperator.k8s.io",
            "v1beta2",
            "sparkapplications",
            client);
    this.scheduledSparkApplicationApi =
        new GenericKubernetesApi<>(
            ScheduledSparkApplication.class,
            ScheduledSparkApplicationList.class,
            "sparkoperator.k8s.io",
            "v1beta2",
            "scheduledsparkapplications",
            client);
  }

  @Override
  public SparkApplication update(SparkApplication app) throws SparkOperatorApiException {
    try {
      return sparkApplicationApi.update(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public SparkApplication create(SparkApplication app) throws SparkOperatorApiException {
    try {
      return sparkApplicationApi.create(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public ScheduledSparkApplication update(ScheduledSparkApplication app)
      throws SparkOperatorApiException {
    try {
      return scheduledSparkApplicationApi.update(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public ScheduledSparkApplication create(ScheduledSparkApplication app)
      throws SparkOperatorApiException {
    try {
      return scheduledSparkApplicationApi.create(app).throwsApiException().getObject();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public List<SparkApplication> list(String namespace, String labelSelector)
      throws SparkOperatorApiException {
    ListOptions options = new ListOptions();
    if (!labelSelector.isEmpty()) {
      options.setLabelSelector(labelSelector);
    }
    try {
      return sparkApplicationApi
          .list(namespace, options)
          .throwsApiException()
          .getObject()
          .getItems();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public Optional<SparkApplication> getSparkApplication(String namespace, String name)
      throws SparkOperatorApiException {
    KubernetesApiResponse<SparkApplication> resp = sparkApplicationApi.get(namespace, name);
    return switch (resp.getHttpStatusCode()) {
      case 200, 404 -> Optional.ofNullable(resp.getObject());
      default -> throw new SparkOperatorApiException(resp.getStatus().toString());
    };
  }

  @Override
  public List<ScheduledSparkApplication> listScheduled(String namespace, String labelSelector)
      throws SparkOperatorApiException {
    ListOptions options = new ListOptions();
    if (!labelSelector.isEmpty()) {
      options.setLabelSelector(labelSelector);
    }
    try {
      return scheduledSparkApplicationApi
          .list(namespace, options)
          .throwsApiException()
          .getObject()
          .getItems();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public Optional<ScheduledSparkApplication> getScheduledSparkApplication(
      String namespace, String name) throws SparkOperatorApiException {
    KubernetesApiResponse<ScheduledSparkApplication> resp =
        scheduledSparkApplicationApi.get(namespace, name);
    return switch (resp.getHttpStatusCode()) {
      case 200, 404 -> Optional.ofNullable(resp.getObject());
      default -> throw new SparkOperatorApiException(resp.getStatus().toString());
    };
  }

  @Override
  public void deleteSparkApplication(String namespace, String name)
      throws SparkOperatorApiException {
    try {
      sparkApplicationApi.delete(namespace, name).throwsApiException();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
  public void deleteScheduledSparkApplication(String namespace, String name)
      throws SparkOperatorApiException {
    try {
      scheduledSparkApplicationApi.delete(namespace, name).throwsApiException();
    } catch (ApiException e) {
      throw new SparkOperatorApiException(e.getMessage());
    }
  }

  @Override
    public Watchable<SparkApplication> watch(String namespace, String labelSelector) throws SparkOperatorApiException {
    ListOptions options = new ListOptions();
    if (!labelSelector.isEmpty()) {
        options.setLabelSelector(labelSelector);
    }
        try {
         Watchable<SparkApplication> watchable = sparkApplicationApi.watch(namespace, options);
         return watchable;
        } catch (ApiException e) {
        throw new SparkOperatorApiException(e.getMessage());
        }
    }
}
