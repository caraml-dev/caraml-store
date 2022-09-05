package dev.caraml.spark.metrics

import java.util
import java.util.Collections
import com.codahale.metrics.{Gauge, Histogram, MetricRegistry, UniformReservoir}
import dev.caraml.spark.UnitSpec

import collection.JavaConverters._

class StatsReporterSpec extends UnitSpec {
  trait Scope {
    val server = new StatsDStub
    val reporter = new StatsdReporterWithTags(
      new MetricRegistry,
      "127.0.0.1",
      server.port
    )

    def gauge[A](v: A): Gauge[A] = new Gauge[A] {
      override def getValue: A = v
    }

    def histogram(values: Seq[Int]): Histogram = {
      val hist = new Histogram(new UniformReservoir)
      values.foreach(hist.update)
      hist
    }
  }

  "Statsd reporter" should "send simple gauge unmodified" in new Scope {
    reporter.report(
      gauges = new util.TreeMap(
        Map(
          "test" -> gauge(0)
        ).asJava
      ),
      counters = Collections.emptySortedMap(),
      histograms = Collections.emptySortedMap(),
      meters = Collections.emptySortedMap(),
      timers = Collections.emptySortedMap()
    )

    server.receive should contain("test:0|g")
  }

  "Statsd reporter" should "keep tags part in the message's end" in new Scope {
    reporter.report(
      gauges = Collections.emptySortedMap(),
      counters = Collections.emptySortedMap(),
      histograms = new util.TreeMap(
        Map(
          "prefix.1111.test#fs=name,job=aaa" -> histogram((1 to 100))
        ).asJava
      ),
      meters = Collections.emptySortedMap(),
      timers = Collections.emptySortedMap()
    )

    server.receive should contain("prefix.test.p95:95.95|ms|#fs:name,job:aaa")
  }
}
