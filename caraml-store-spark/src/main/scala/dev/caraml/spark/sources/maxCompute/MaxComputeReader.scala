package dev.caraml.spark.sources.maxCompute

import dev.caraml.spark.{MaxComputeSource, MaxComputeConfig}
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.jdbc.JdbcDialects
import com.caraml.odps.CustomDialect
import org.apache.log4j.Logger

object MaxComputeReader {
  private val logger = Logger.getLogger(getClass.getCanonicalName)
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
      "jdbc:odps:%s/?project=%s&interactiveMode=%s&enableLimit=%s&autoSelectLimit=%s&enableOdpsLogger=true&alwaysFallback=true"
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
    logger.info(s"MaxCompute JDBC Connection URL: $maxComputeJDBCConnectionURL")
    logger.info(s"MaxCompute SQL Query: $sqlQuery")

    val customDialect = new CustomDialect()
    JdbcDialects.registerDialect(customDialect)

    sparkSession.read
      .format("jdbc")
      .option("url", maxComputeJDBCConnectionURL)
      .option("sessionInitStatement", "set odps.stage.reducer.num=50")
      // Not setting queryTimeout will fail the query, whereas setting it up actually doesn't make an impact
      .option("queryTimeout", 5000)
      .option("dbtable", sqlQuery)
      .option("user", maxComputeAccessID)
      .option("password", maxComputeAccessKey)
      .option("partitionColumn", source.eventTimestampColumn)
      .option("lowerBound", start.toString())
      .option("upperBound", end.toString())
      .option("numPartitions", config.numPartitions)
      .option("fetchsize", config.fetchSize)
      .load()

  }
}
