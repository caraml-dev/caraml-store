package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.ScheduledSparkApplication;
import dev.caraml.store.sparkjob.crd.ScheduledSparkApplicationList;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import dev.caraml.store.sparkjob.crd.SparkApplicationList;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Optional;

public class SparkOperatorApiImpl implements SparkOperatorApi {

  private final GenericKubernetesApi<SparkApplication, SparkApplicationList> sparkApplicationApi;
  private final GenericKubernetesApi<ScheduledSparkApplication, ScheduledSparkApplicationList>
      scheduledSparkApplicationApi;

  public SparkOperatorApiImpl(ClusterConfig.Cluster cluster) throws IOException {
    // Note: intentionally does NOT call Configuration.setDefaultApiClient so that
    // multiple clusters can hold independent clients simultaneously.
    ApiClient client = buildClient(cluster);
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

  private static ApiClient buildClient(ClusterConfig.Cluster cluster) throws IOException {
    if (Boolean.TRUE.equals(cluster.getInCluster())) {
      return ClientBuilder.cluster().build();
    }
    String context = cluster.getContext();
    if (context != null && !context.isEmpty()) {
      File configFile = kubeConfigFile();
      if (configFile == null) {
        throw new IOException(
            String.format(
                "No kubeconfig found ($KUBECONFIG or ~/.kube/config) to resolve context '%s'",
                context));
      }
      try (Reader reader = new FileReader(configFile)) {
        KubeConfig kubeConfig = KubeConfig.loadKubeConfig(reader);
        if (!kubeConfig.setContext(context)) {
          throw new IOException(String.format("kubeconfig context '%s' not found", context));
        }
        // Ensure relative certificate/key paths in the kubeconfig resolve correctly.
        kubeConfig.setFile(configFile);
        return ClientBuilder.kubeconfig(kubeConfig).build();
      }
    }
    return Config.defaultClient();
  }

  private static File kubeConfigFile() {
    // NB: Config.ENV_KUBECONFIG is the env var name ("KUBECONFIG"); KubeConfig.KUBECONFIG is the
    // config *filename* ("config"). Reuse the library constant to avoid confusing the two.
    return resolveKubeConfigFile(System.getenv(Config.ENV_KUBECONFIG), homeDir());
  }

  // Resolve the home directory the same way io.kubernetes.client.util.ClientBuilder does:
  // prefer $HOME, then the user.home system property.
  private static String homeDir() {
    String home = System.getenv("HOME");
    if (home != null && !home.isEmpty()) {
      return home;
    }
    return System.getProperty("user.home");
  }

  /**
   * Locates the kubeconfig file, mirroring {@code io.kubernetes.client.util.ClientBuilder}: the
   * first path listed in {@code $KUBECONFIG} if it exists, otherwise {@code <home>/.kube/config} if
   * it exists, otherwise {@code null}. Package-private for testing.
   */
  static File resolveKubeConfigFile(String kubeConfigEnv, String homeDir) {
    if (kubeConfigEnv != null && !kubeConfigEnv.isEmpty()) {
      // $KUBECONFIG may list multiple files; the client library uses the first entry.
      File fromEnv = new File(kubeConfigEnv.split(File.pathSeparator)[0]);
      if (fromEnv.exists()) {
        return fromEnv;
      }
    }
    if (homeDir != null && !homeDir.isEmpty()) {
      File fromHome = new File(new File(homeDir, KubeConfig.KUBEDIR), KubeConfig.KUBECONFIG);
      if (fromHome.exists()) {
        return fromHome;
      }
    }
    return null;
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
}
