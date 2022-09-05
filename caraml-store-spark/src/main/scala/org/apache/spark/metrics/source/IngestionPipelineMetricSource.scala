package org.apache.spark.metrics.source

class IngestionPipelineMetricSource extends BaseMetricSource {
  override val sourceName: String = IngestionPipelineMetricSource.sourceName

  val METRIC_DEADLETTER_ROWS_INSERTED =
    metricRegistry.counter(counterWithLabels("deadletter_count"))

  val METRIC_ROWS_READ_FROM_SOURCE =
    metricRegistry.counter(counterWithLabels("read_from_source_count"))
}

object IngestionPipelineMetricSource {
  val sourceName = "ingestion_pipeline"
}
