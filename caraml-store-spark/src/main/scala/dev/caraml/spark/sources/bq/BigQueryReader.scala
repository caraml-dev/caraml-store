package dev.caraml.spark.sources.bq

import dev.caraml.spark.{BQConfig, BQSource}

import java.sql.Timestamp
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col

object BigQueryReader {
  def createBatchSource(
      sqlContext: SQLContext,
      source: BQSource,
      bq: Option[BQConfig],
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val reader = sqlContext.read
      .format("bigquery")
      .option("viewsEnabled", "true")

    bq.foreach(config =>
      config.materialization.foreach(materializationConfig =>
        reader
          .option("materializationProject", materializationConfig.project)
          .option("materializationDataset", materializationConfig.dataset)
      )
    )

    reader
      .load(s"${source.project}.${source.dataset}.${source.table}")
      .filter(col(source.eventTimestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.eventTimestampColumn) < new Timestamp(end.getMillis))
  }
}
