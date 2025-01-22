package dev.caraml.spark.sources.maxCompute

import dev.caraml.spark.{MaxComputeSource}

import java.sql.Timestamp
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object MaxComputeReader {
  def createBatchSource(
       sparkSession: SparkSession,
       source: MaxComputeSource,
       start: DateTime,
       end: DateTime
     ): DataFrame = {

    val ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource.DefaultSource"
    val reader = sparkSession.read.format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", "g_gojek_id_staging")
      .option("spark.hadoop.odps.access.id", "access_id")
      .option("spark.hadoop.odps.access.key", "access_key")
      .option("spark.hadoop.odps.end.point", "endpoint")
      .option("spark.hadoop.odps.table.name", "table name")

    reader.load(s"${source.project}.${source.dataset}.${source.table}")
      .filter(col(source.eventTimestampColumn) >= new Timestamp(start.getMillis))
      .filter(col(source.eventTimestampColumn) < new Timestamp(end.getMillis))

//    val data = sparkSession.sql("select * from table where timestamp >= 100 && timestamp <= 100")
//    data.toDF()

  }
}
