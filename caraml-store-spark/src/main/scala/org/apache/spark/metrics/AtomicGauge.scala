package org.apache.spark.metrics

import com.codahale.metrics.Gauge

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

class AtomicLongGauge(initialValue: Long = 0L) extends Gauge[Long] {
  val value                   = new AtomicLong(initialValue)
  override def getValue: Long = value.get()
}

class AtomicIntegerGauge(initialValue: Int = 0) extends Gauge[Int] {
  val value                  = new AtomicInteger(initialValue)
  override def getValue: Int = value.get()
}
