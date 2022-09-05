package org.apache.spark.metrics.source

class BigTableSinkMetricSource extends BaseMetricSource {
  override val sourceName: String = BigTableSinkMetricSource.sourceName

  val METRIC_TOTAL_ROWS_INSERTED =
    metricRegistry.counter(counterWithLabels("feature_row_ingested_count"))

  val METRIC_ROWS_LAG =
    metricRegistry.histogram(metricWithLabels("feature_row_lag_ms"))
}

object BigTableSinkMetricSource {
  val sourceName = "bigtable_sink"
}
