package dev.caraml.store.mlp;

import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import io.github.resilience4j.retry.annotation.Retry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(prefix = "caraml.mlp", name = "enabled", havingValue = "true")
public class ApiClient implements ProjectProvider {

  private final HttpClient httpClient;
  private final String endpoint;
  private final Boolean authEnabled;
  private final String authTargetAudience;
  private final Integer requestTimeOutMs;

  @Autowired
  public ApiClient(ClientConfig config) {
    httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectionTimeOutMs()))
            .build();
    endpoint = config.getEndpoint();
    authEnabled = config.getAuthEnabled();
    authTargetAudience = config.getAuthTargetAudience();
    requestTimeOutMs = config.getRequestTimeOutMs();
  }

  private String getIdTokenFromMetadataServer() throws IOException, InterruptedException {
    GoogleCredentials googleCredentials = GoogleCredentials.getApplicationDefault();

    IdTokenCredentials idTokenCredentials =
        IdTokenCredentials.newBuilder()
            .setIdTokenProvider((IdTokenProvider) googleCredentials)
            .setTargetAudience(authTargetAudience)
            .build();

    return idTokenCredentials.refreshAccessToken().getTokenValue();
  }

  @Retry(name = "listProject")
  public HttpResponse<String> sendListProjectRequest() {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .timeout(Duration.ofMillis(requestTimeOutMs))
            .uri(URI.create(String.format("%s/v1/projects", endpoint)))
            .GET();
    if (authEnabled) {
      String token;
      try {
        token = getIdTokenFromMetadataServer();
      } catch (IOException | InterruptedException e) {
        throw new FailedRequestException("Unable to retrieve id token", e);
      }
      requestBuilder.header("Authorization", String.format("Bearer %s", token));
    }
    HttpRequest request = requestBuilder.build();
    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      throw new FailedRequestException("Unable to connect to MLP API service", e);
    }

    if (response.statusCode() != HTTP_OK) {
      throw new FailedRequestException(
          String.format("non-200 response from MLP console: %s", response.body()));
    }

    return response;
  }

  @Override
  public List<Project> listProjects() {

    HttpResponse<String> response = sendListProjectRequest();
    ObjectMapper objectMapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      CollectionType collectionType =
          TypeFactory.defaultInstance().constructCollectionType(List.class, Project.class);
      return objectMapper.readValue(response.body(), collectionType);
    } catch (JsonProcessingException e) {
      throw new FailedRequestException(
          String.format(
              "failed to parse response from MLP console. Response body: %s", response.body()));
    }
  }

  @Override
  public Project getProject(String name) {
    return listProjects().stream()
        .filter(project -> project.name().equals(name))
        .findFirst()
        .orElseThrow(
            () -> new FailedRequestException(String.format("Project %s does not exist", name)));
  }
}
