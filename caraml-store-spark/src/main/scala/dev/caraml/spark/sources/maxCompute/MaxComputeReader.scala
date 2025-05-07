package dev.caraml.spark.sources.maxCompute

import dev.caraml.spark.{MaxComputeSource, MaxComputeConfig}
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.jdbc.JdbcDialects
import com.caraml.odps.CustomDialect

object MaxComputeReader {
  def createBatchSource(
      sparkSession: SparkSession,
      source: MaxComputeSource,
      maxComputeConfig: Option[MaxComputeConfig],
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val maxComputeAccessID  = sys.env("CARAML_SPARK_MAXCOMPUTE_ACCESS_ID")
    val maxComputeAccessKey = sys.env("CARAML_SPARK_MAXCOMPUTE_ACCESS_KEY")

    val config = maxComputeConfig.getOrElse(MaxComputeConfig())

    val maxComputeJDBCConnectionURL =
      "jdbc:odps:%s/?project=%s&interactiveMode=%s&enableLimit=%s&autoSelectLimit=%s&enableOdpsLogger=true"
        .format(
          config.endpoint,
          source.project,
          config.interactiveMode,
          config.enableLimit,
          config.autoSelectLimit
        )

    val sqlQuery =
      "(select * from `%s.%s` where %s >= cast(to_date('%s','yyyy-mm-ddThh:mi:ss.ff3Z') as timestamp) and %s < cast(to_date('%s','yyyy-mm-ddThh:mi:ss.ff3Z') as timestamp))" format (
        source.dataset, source.table, source.eventTimestampColumn, start, source.eventTimestampColumn, end
      )
    print(sqlQuery)

    val customDialect = new CustomDialect()
    JdbcDialects.registerDialect(customDialect)

    val data = sparkSession.read
      .format("jdbc")
      .option("url", maxComputeJDBCConnectionURL)
      // Not setting queryTimeout will fail the query, whereas setting it up actually doesn't make an impact
      .option("queryTimeout", 5000)
      .option("dbtable", sqlQuery)
      .option("user", maxComputeAccessID)
      .option("password", maxComputeAccessKey)
      .option("queryTimeout", 21600)
      .load()

    data.toDF()
  }
}
