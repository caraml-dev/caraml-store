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

    val data = sparkSession.sql("select * from table where timestamp >= 100 && timestamp <= 100")
    data.toDF()
  }
}
