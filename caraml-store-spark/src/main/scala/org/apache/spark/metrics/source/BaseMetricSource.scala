package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SparkEnv

class BaseMetricSource extends Source {
  override val sourceName: String = ""

  override val metricRegistry: MetricRegistry = new MetricRegistry

  private val sparkConfig = SparkEnv.get.conf

  private val metricLabels = sparkConfig.get("spark.metrics.labels", "")

  private val appId = sparkConfig.get("spark.app.id", "")

  private val executorId = sparkConfig.get("spark.executor.id", "")

  protected def metricWithLabels(name: String) = {
    if (metricLabels.isEmpty) {
      name
    } else {
      s"$name#$metricLabels,job_id=$appId-$executorId"
    }
  }

  protected def counterWithLabels(name: String) = {
    if (metricLabels.isEmpty) {
      name
    } else {
      s"$name#$metricLabels"
    }
  }

  protected def gaugeWithLabels(name: String) = {
    if (metricLabels.isEmpty) {
      name
    } else {
      s"$name#$metricLabels"
    }
  }
}
