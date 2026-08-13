package dev.caraml.store.sparkjob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkOperatorApiImplTest {

  @Test
  public void shouldUseKubeconfigEnvWhenFileExists(@TempDir Path tmp) throws IOException {
    Path config = Files.createFile(tmp.resolve("kubeconfig"));
    File resolved =
        SparkOperatorApiImpl.resolveKubeConfigFile(config.toString(), "/nonexistent-home");
    assertEquals(config.toFile(), resolved);
  }

  @Test
  public void shouldUseFirstEntryWhenKubeconfigEnvHasMultiplePaths(@TempDir Path tmp)
      throws IOException {
    Path first = Files.createFile(tmp.resolve("first"));
    Path second = Files.createFile(tmp.resolve("second"));
    String env = first + File.pathSeparator + second;
    assertEquals(first.toFile(), SparkOperatorApiImpl.resolveKubeConfigFile(env, "/nonexistent"));
  }

  @Test
  public void shouldFallBackToHomeWhenKubeconfigEnvFileMissing(@TempDir Path tmp)
      throws IOException {
    Path config = Files.createFile(Files.createDirectories(tmp.resolve(".kube")).resolve("config"));
    File resolved = SparkOperatorApiImpl.resolveKubeConfigFile("/does/not/exist", tmp.toString());
    assertEquals(config.toFile(), resolved);
  }

  @Test
  public void shouldFallBackToHomeWhenKubeconfigEnvNull(@TempDir Path tmp) throws IOException {
    Path config = Files.createFile(Files.createDirectories(tmp.resolve(".kube")).resolve("config"));
    assertEquals(config.toFile(), SparkOperatorApiImpl.resolveKubeConfigFile(null, tmp.toString()));
  }

  @Test
  public void shouldReturnNullWhenNothingExists(@TempDir Path tmp) {
    File resolved =
        SparkOperatorApiImpl.resolveKubeConfigFile(
            "/does/not/exist", tmp.resolve("empty-home").toString());
    assertNull(resolved);
  }
}
