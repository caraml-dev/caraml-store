package org.apache.spark.metrics.source

import org.apache.spark.metrics.AtomicLongGauge
import org.apache.spark.sql.streaming.StreamingQueryProgress

import java.time.Instant

class StreamingMetricSource extends BaseMetricSource {
  override val sourceName: String = StreamingMetricSource.sourceName

  private val BATCH_DURATION_GAUGE =
    metricRegistry.register(gaugeWithLabels("batch_duration_ms"), new AtomicLongGauge())
  private val PROCESSED_ROWS_PER_SECOND_GAUGE =
    metricRegistry.register(gaugeWithLabels("input_rows_per_second"), new AtomicLongGauge())
  private val INPUT_ROWS_PER_SECOND_GAUGE =
    metricRegistry.register(gaugeWithLabels("processed_rows_per_second"), new AtomicLongGauge())
  private val LAST_CONSUMED_KAFKA_TIMESTAMP_GAUGE =
    metricRegistry.register(gaugeWithLabels("last_consumed_kafka_timestamp"), new AtomicLongGauge())
  private val LAST_PROCESSED_EVENT_TIMESTAMP_GAUGE =
    metricRegistry.register(
      gaugeWithLabels("last_processed_event_timestamp"),
      new AtomicLongGauge()
    )

  def updateStreamingProgress(progress: StreamingQueryProgress): Unit = {
    BATCH_DURATION_GAUGE.value.set(progress.batchDuration)
    INPUT_ROWS_PER_SECOND_GAUGE.value.set(progress.inputRowsPerSecond.toLong)
    PROCESSED_ROWS_PER_SECOND_GAUGE.value.set(progress.processedRowsPerSecond.toLong)

    val epochTimestamp = Instant.parse(progress.timestamp).getEpochSecond
    LAST_PROCESSED_EVENT_TIMESTAMP_GAUGE.value.set(epochTimestamp)
  }

  def updateKafkaTimestamp(timestamp: Long): Unit = {
    LAST_CONSUMED_KAFKA_TIMESTAMP_GAUGE.value.set(timestamp)
  }
}

object StreamingMetricSource {
  val sourceName = "streaming"
}
