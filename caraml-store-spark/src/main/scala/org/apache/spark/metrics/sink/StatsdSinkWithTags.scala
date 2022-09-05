package org.apache.spark.metrics.sink

import com.codahale.metrics.MetricRegistry
import dev.caraml.spark.metrics.StatsdReporterWithTags
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem

import java.util.Properties
import java.util.concurrent.TimeUnit

class StatsdSinkWithTags(
    val property: Properties,
    val registry: MetricRegistry,
    securityMgr: SecurityManager
) extends Sink
    with Logging {
  import StatsdSink._

  val host = property.getProperty(STATSD_KEY_HOST, STATSD_DEFAULT_HOST)
  val port = property.getProperty(STATSD_KEY_PORT, STATSD_DEFAULT_PORT).toInt

  val pollPeriod = property.getProperty(STATSD_KEY_PERIOD, STATSD_DEFAULT_PERIOD).toInt
  val pollUnit =
    TimeUnit.valueOf(property.getProperty(STATSD_KEY_UNIT, STATSD_DEFAULT_UNIT).toUpperCase)

  val prefix = property.getProperty(STATSD_KEY_PREFIX, STATSD_DEFAULT_PREFIX)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter = new StatsdReporterWithTags(registry, host, port, prefix)

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
    logInfo(s"StatsdSink started with prefix: '$prefix'")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("StatsdSink stopped.")
  }

  override def report(): Unit = reporter.report()
}
