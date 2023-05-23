package dev.caraml.store.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import dev.caraml.store.mlp.ApiClient;
import dev.caraml.store.mlp.ClientConfig;
import dev.caraml.store.mlp.Project;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class ApiClientTest {

  public static final DockerImageName MOCKSERVER_IMAGE =
      DockerImageName.parse("mockserver/mockserver")
          .withTag("mockserver-" + MockServerClient.class.getPackage().getImplementationVersion());

  private static MockServerClient mockServerClient;
  private static ApiClient apiClient;

  @Container
  public static MockServerContainer mockServerContainer = new MockServerContainer(MOCKSERVER_IMAGE);

  @BeforeAll
  public static void globalSetup() {
    mockServerClient =
        new MockServerClient(mockServerContainer.getHost(), mockServerContainer.getServerPort());
    mockServerClient
        .when(request().withPath("/v1/projects"))
        .respond(
            response()
                .withBody(
                    "[{\"id\": 1, \"name\": \"default\", \"team\": \"team\", \"stream\": \"stream\"},"
                        + "{\"id\": 2, \"name\": \"project1\", \"team\": \"team\", \"stream\": \"stream\"},"
                        + "{\"id\": 3, \"name\": \"project2\", \"team\": \"team\", \"stream\": \"stream\"},"
                        + "{\"id\": 4, \"name\": \"archived\", \"team\": \"team\", \"stream\": \"stream\"},"
                        + "{\"id\": 5, \"name\": \"new_project\", \"team\": \"team\", \"stream\": \"stream\"}]"));
  }

  @BeforeEach
  public void initState() {
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.setAuthTargetAudience("audience");
    clientConfig.setRequestTimeOutMs(3000);
    clientConfig.setConnectionTimeOutMs(3000);
    clientConfig.setEndpoint(mockServerContainer.getEndpoint());
    clientConfig.setAuthEnabled(false);
    apiClient = new ApiClient(clientConfig);
  }

  @Test
  public void testRequestForProject() {
    Optional<Project> existingProject = apiClient.getProject("project1");
    assertTrue(existingProject.isPresent());
    assertThat(existingProject.get().name(), equalTo("project1"));
    Optional<Project> missingProject = apiClient.getProject("projectx");
    assertTrue(missingProject.isEmpty());
  }
}
