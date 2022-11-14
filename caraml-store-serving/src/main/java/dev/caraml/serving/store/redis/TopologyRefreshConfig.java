package dev.caraml.serving.store.redis;

import lombok.Getter;
import lombok.Setter;

/**
 * TopologyRefreshConfig configure redis client behavior when there is change in redis cluster
 * topology. Refer to:
 * https://github.com/lettuce-io/lettuce-core/wiki/Client-options#cluster-specific-options
 */
@Getter
@Setter
public class TopologyRefreshConfig {
  private final boolean enableAllAdaptiveTriggerRefresh;
  private final boolean enablePeriodicRefresh;
  private final int refreshPeriodSecond;

  public static final TopologyRefreshConfig DEFAULT = new TopologyRefreshConfig(true, false, 30);

  public TopologyRefreshConfig(
      boolean enableAllAdaptiveTriggerRefresh,
      boolean enablePeriodicRefresh,
      int refreshPeriodSecond) {
    this.enableAllAdaptiveTriggerRefresh = enableAllAdaptiveTriggerRefresh;
    this.enablePeriodicRefresh = enablePeriodicRefresh;
    this.refreshPeriodSecond = refreshPeriodSecond;
  }
}
