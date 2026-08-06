package dev.caraml.store.sparkjob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import org.junit.jupiter.api.Test;

public class SparkOperatorApiImplTest {

  @Test
  public void shouldUseKubeconfigEnvWhenSet() {
    File resolved =
        SparkOperatorApiImpl.resolveKubeConfigFile("/etc/tker-i-models-01/kubeconfig", "/root");
    assertEquals(new File("/etc/tker-i-models-01/kubeconfig"), resolved);
  }

  @Test
  public void shouldUseFirstEntryWhenKubeconfigEnvHasMultiplePaths() {
    String env = "/etc/first/kubeconfig" + File.pathSeparator + "/etc/second/kubeconfig";
    File resolved = SparkOperatorApiImpl.resolveKubeConfigFile(env, "/root");
    assertEquals(new File("/etc/first/kubeconfig"), resolved);
  }

  @Test
  public void shouldFallBackToHomeDirWhenKubeconfigEnvNull() {
    File resolved = SparkOperatorApiImpl.resolveKubeConfigFile(null, "/home/caraml");
    assertEquals(new File("/home/caraml/.kube/config"), resolved);
  }

  @Test
  public void shouldFallBackToHomeDirWhenKubeconfigEnvEmpty() {
    File resolved = SparkOperatorApiImpl.resolveKubeConfigFile("", "/home/caraml");
    assertEquals(new File("/home/caraml/.kube/config"), resolved);
  }
}
