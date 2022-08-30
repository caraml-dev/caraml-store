package dev.caraml.spark.sources.file

import dev.caraml.spark.FileSource

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime

object FileReader {
  def createBatchSource(
      sqlContext: SQLContext,
      source: FileSource,
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val reader = sqlContext.read
      .parquet(source.path)
      .filter(col(source.eventTimestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.eventTimestampColumn) < new Timestamp(end.getMillis))

    source.datePartitionColumn match {
      case Some(partitionColumn) if partitionColumn.nonEmpty =>
        reader
          .filter(col(partitionColumn) >= new Date(start.getMillis))
          .filter(col(partitionColumn) <= new Date(end.getMillis))
      case _ => reader
    }
  }
}
