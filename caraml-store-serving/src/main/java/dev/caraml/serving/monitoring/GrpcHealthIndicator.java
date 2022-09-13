package dev.caraml.serving.monitoring;

import static io.grpc.health.v1.HealthGrpc.newBlockingStub;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc.HealthBlockingStub;
import net.devh.boot.grpc.server.config.GrpcServerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class GrpcHealthIndicator implements HealthIndicator {

  private final HealthBlockingStub healthStub;

  @Autowired
  public GrpcHealthIndicator(GrpcServerProperties serverProperties) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", serverProperties.getPort())
            .usePlaintext()
            .build();
    this.healthStub = newBlockingStub(channel);
  }

  @Override
  public Health health() {
    HealthCheckResponse check = healthStub.check(HealthCheckRequest.newBuilder().build());
    Health.Builder healthCheckBuilder =
        check.getStatus() == HealthCheckResponse.ServingStatus.SERVING
            ? Health.up()
            : Health.down();
    return healthCheckBuilder.build();
  }
}
