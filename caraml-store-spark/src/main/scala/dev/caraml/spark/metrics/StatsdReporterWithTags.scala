package dev.caraml.spark.metrics

import java.io.IOException
import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.SortedMap
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import com.codahale.metrics._
import org.apache.hadoop.net.NetUtils

import org.apache.spark.internal.Logging

/**
  * @see <a href="https://github.com/etsy/statsd/blob/master/docs/metric_types.md">
  *      StatsD metric types</a>
  */
object StatsdMetricType {
  val COUNTER = "c"
  val GAUGE   = "g"
  val TIMER   = "ms"
  val Set     = "s"
}

/**
  * This is fork of internal spark implementation of StatsdReporter.
  * The main difference between this implementation & spark is support of tags inside metric names.
  *
  * When we extend metric name (by adding some suffix) we must keep tags part in the end of the name.
  * Labels are separated from name by # (see tagging extensions of prometheus exporter
  * https://github.com/prometheus/statsd_exporter#tagging-extensions)
  * E.g.,
  *
  * metric_name#tag=value + count -> metric_name.count#tag=value
  */
class StatsdReporterWithTags(
    registry: MetricRegistry,
    host: String = "127.0.0.1",
    port: Int = 8125,
    prefix: String = "",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS
) extends ScheduledReporter(registry, "statsd-reporter", filter, rateUnit, durationUnit)
    with Logging {

  import StatsdMetricType._

  private val address    = new InetSocketAddress(host, port)
  private val whitespace = "[\\s]+".r

  override def report(
      gauges: SortedMap[String, Gauge[_]],
      counters: SortedMap[String, Counter],
      histograms: SortedMap[String, Histogram],
      meters: SortedMap[String, Meter],
      timers: SortedMap[String, Timer]
  ): Unit =
    Try(new DatagramSocket) match {
      case Failure(ioe: IOException) =>
        logWarning(
          "StatsD datagram socket construction failed",
          NetUtils.wrapException(host, port, NetUtils.getHostname(), 0, ioe)
        )
      case Failure(e) => logWarning("StatsD datagram socket construction failed", e)
      case Success(s) =>
        implicit val socket = s
        val localAddress    = Try(socket.getLocalAddress).map(_.getHostAddress).getOrElse(null)
        val localPort       = socket.getLocalPort
        Try {
          gauges.entrySet.asScala.foreach(e => reportGauge(e.getKey, e.getValue))
          counters.entrySet.asScala.foreach(e => reportCounter(e.getKey, e.getValue))
          histograms.entrySet.asScala.foreach(e => reportHistogram(e.getKey, e.getValue))
          meters.entrySet.asScala.foreach(e => reportMetered(e.getKey, e.getValue))
          timers.entrySet.asScala.foreach(e => reportTimer(e.getKey, e.getValue))
        } recover {
          case ioe: IOException =>
            logDebug(
              s"Unable to send packets to StatsD",
              NetUtils.wrapException(
                address.getHostString,
                address.getPort,
                localAddress,
                localPort,
                ioe
              )
            )
          case e: Throwable => logDebug(s"Unable to send packets to StatsD at '$host:$port'", e)
        }
        Try(socket.close()) recover {
          case ioe: IOException =>
            logDebug(
              "Error when close socket to StatsD",
              NetUtils.wrapException(
                address.getHostString,
                address.getPort,
                localAddress,
                localPort,
                ioe
              )
            )
          case e: Throwable => logDebug("Error when close socket to StatsD", e)
        }
    }

  private def reportGauge(name: String, gauge: Gauge[_])(implicit socket: DatagramSocket): Unit =
    formatAny(gauge.getValue).foreach(v => send(fullName(name), v, GAUGE))

  private def reportCounter(name: String, counter: Counter)(implicit
      socket: DatagramSocket
  ): Unit = {
    val snapshot = counter.getCount
    send(fullName(name), format(snapshot), COUNTER)
    counter.dec(snapshot) // reset counter
  }

  private def reportHistogram(name: String, histogram: Histogram)(implicit
      socket: DatagramSocket
  ): Unit = {
    val snapshot = histogram.getSnapshot
    send(fullName(name, "count"), format(histogram.getCount), GAUGE)
    send(fullName(name, "max"), format(snapshot.getMax), TIMER)
    send(fullName(name, "mean"), format(snapshot.getMean), TIMER)
    send(fullName(name, "min"), format(snapshot.getMin), TIMER)
    send(fullName(name, "stddev"), format(snapshot.getStdDev), TIMER)
    send(fullName(name, "p50"), format(snapshot.getMedian), TIMER)
    send(fullName(name, "p75"), format(snapshot.get75thPercentile), TIMER)
    send(fullName(name, "p95"), format(snapshot.get95thPercentile), TIMER)
    send(fullName(name, "p98"), format(snapshot.get98thPercentile), TIMER)
    send(fullName(name, "p99"), format(snapshot.get99thPercentile), TIMER)
    send(fullName(name, "p999"), format(snapshot.get999thPercentile), TIMER)
  }

  private def reportMetered(name: String, meter: Metered)(implicit socket: DatagramSocket): Unit = {
    send(fullName(name, "count"), format(meter.getCount), GAUGE)
    send(fullName(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate)), TIMER)
    send(fullName(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate)), TIMER)
    send(fullName(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate)), TIMER)
    send(fullName(name, "mean_rate"), format(convertRate(meter.getMeanRate)), TIMER)
  }

  private def reportTimer(name: String, timer: Timer)(implicit socket: DatagramSocket): Unit = {
    val snapshot = timer.getSnapshot
    send(fullName(name, "max"), format(convertDuration(snapshot.getMax)), TIMER)
    send(fullName(name, "mean"), format(convertDuration(snapshot.getMean)), TIMER)
    send(fullName(name, "min"), format(convertDuration(snapshot.getMin)), TIMER)
    send(fullName(name, "stddev"), format(convertDuration(snapshot.getStdDev)), TIMER)
    send(fullName(name, "p50"), format(convertDuration(snapshot.getMedian)), TIMER)
    send(fullName(name, "p75"), format(convertDuration(snapshot.get75thPercentile)), TIMER)
    send(fullName(name, "p95"), format(convertDuration(snapshot.get95thPercentile)), TIMER)
    send(fullName(name, "p98"), format(convertDuration(snapshot.get98thPercentile)), TIMER)
    send(fullName(name, "p99"), format(convertDuration(snapshot.get99thPercentile)), TIMER)
    send(fullName(name, "p999"), format(convertDuration(snapshot.get999thPercentile)), TIMER)

    reportMetered(name, timer)
  }

  private val nameWithTag = """(\S+)#(\S+)""".r

  private def send(name: String, value: String, metricType: String)(implicit
      socket: DatagramSocket
  ): Unit = {
    val bytes = name match {
      case nameWithTag(name, tags) =>
        val tagsWithSemicolon = tags.replace('=', ':')
        sanitize(s"$name:$value|$metricType|#$tagsWithSemicolon").getBytes(UTF_8)
      case _ =>
        sanitize(s"$name:$value|$metricType").getBytes(UTF_8)
    }
    val packet = new DatagramPacket(bytes, bytes.length, address)
    socket.send(packet)
  }

  private def fullName(name: String, suffixes: String*): String = name match {
    case nameWithTag(name, tags) =>
      // filter out parts that consists only from numbers
      // that could be executor-id for example
      val stableName = name.split('.').filterNot(_ forall Character.isDigit).mkString(".")
      MetricRegistry.name(prefix, stableName +: suffixes: _*) ++ "#" ++ tags
    case _ =>
      MetricRegistry.name(prefix, name +: suffixes: _*)
  }

  private def sanitize(s: String): String = whitespace.replaceAllIn(s, "-")

  private def format(v: Any): String = formatAny(v).getOrElse("")

  private def formatAny(v: Any): Option[String] =
    v match {
      case f: Float      => Some("%2.2f".format(f))
      case d: Double     => Some("%2.2f".format(d))
      case b: BigDecimal => Some("%2.2f".format(b))
      case n: Number     => Some(n.toString)
      case _             => None
    }
}
