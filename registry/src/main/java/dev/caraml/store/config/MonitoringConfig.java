package dev.caraml.store.config;

import io.prometheus.client.exporter.MetricsServlet;
import javax.servlet.http.HttpServlet;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MonitoringConfig {

  private static final String PROMETHEUS_METRICS_PATH = "/metrics";

  /**
   * Add Prometheus exposition to an existing HTTP server using servlets.
   *
   * <p>https://github.com/prometheus/client_java/tree/b61dd232a504e20dad404a2bf3e2c0b8661c212a#http
   *
   * @return HTTP servlet for returning metrics data
   */
  @Bean
  public ServletRegistrationBean<HttpServlet> metricsServlet() {
    return new ServletRegistrationBean<>(new MetricsServlet(), PROMETHEUS_METRICS_PATH);
  }
}
