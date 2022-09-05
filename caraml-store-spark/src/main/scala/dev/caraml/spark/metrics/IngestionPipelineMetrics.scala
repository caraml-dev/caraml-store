package dev.caraml.spark.metrics

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.IngestionPipelineMetricSource
import org.apache.spark.sql.Row

class IngestionPipelineMetrics extends Serializable {

  def incrementDeadLetters(row: Row): Row = {
    metricSource.foreach(_.METRIC_DEADLETTER_ROWS_INSERTED.inc())
    row
  }

  def incrementRead(row: Row): Row = {
    metricSource.foreach(_.METRIC_ROWS_READ_FROM_SOURCE.inc())
    row
  }

  private lazy val metricSource: Option[IngestionPipelineMetricSource] = {
    val metricsSystem = SparkEnv.get.metricsSystem
    IngestionPipelineMetricsLock.synchronized {
      if (metricsSystem.getSourcesByName(IngestionPipelineMetricSource.sourceName).isEmpty) {
        metricsSystem.registerSource(new IngestionPipelineMetricSource)
      }
    }

    metricsSystem.getSourcesByName(IngestionPipelineMetricSource.sourceName) match {
      case Seq(head) => Some(head.asInstanceOf[IngestionPipelineMetricSource])
      case _         => None
    }
  }
}

private object IngestionPipelineMetricsLock
