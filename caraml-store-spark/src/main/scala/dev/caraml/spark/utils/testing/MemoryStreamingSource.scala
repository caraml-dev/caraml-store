package dev.caraml.spark.utils.testing

import dev.caraml.spark.{DataFormat, StreamingSource}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream

// For test purposes
case class MemoryStreamingSource(
    stream: MemoryStream[_],
    override val fieldMapping: Map[String, String] = Map.empty,
    override val eventTimestampColumn: String = "timestamp",
    override val createdTimestampColumn: Option[String] = None,
    override val datePartitionColumn: Option[String] = None
) extends StreamingSource {
  def read: DataFrame = stream.toDF()

  override def format: DataFormat = null
}
