package dev.caraml.spark.metrics

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.StreamingMetricSource
import org.apache.spark.sql.streaming.StreamingQueryProgress

class StreamingMetrics extends Serializable {

  private val metricSource: Option[StreamingMetricSource] = {
    val metricsSystem = SparkEnv.get.metricsSystem

    metricsSystem.getSourcesByName(StreamingMetricSource.sourceName) match {
      case Seq(head) => Some(head.asInstanceOf[StreamingMetricSource])
      case _         => None
    }
  }

  def updateStreamingProgress(
      progress: StreamingQueryProgress
  ): Unit = {
    metricSource.foreach(_.updateStreamingProgress(progress))
  }

  def updateKafkaTimestamp(timestamp: Long): Unit = {
    metricSource.foreach(_.updateKafkaTimestamp(timestamp))
  }
}

private object StreamingMetricsLock
