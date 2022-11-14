package dev.caraml.serving.store.redis;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TCPConfig {
  private final Integer keepIdle;
  private final Integer keepInterval;
  private final Integer keepConnection;
  private final Integer userTimeout;

  public TCPConfig(
      Integer keepIdle, Integer keepInterval, Integer keepConnection, Integer userTimeout) {
    this.keepIdle = keepIdle;
    this.keepInterval = keepInterval;
    this.keepConnection = keepConnection;
    this.userTimeout = userTimeout;
  }
}
