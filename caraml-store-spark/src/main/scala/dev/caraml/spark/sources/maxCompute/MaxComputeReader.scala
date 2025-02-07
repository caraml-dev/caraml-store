package dev.caraml.spark.sources.maxCompute

import dev.caraml.spark.MaxComputeSource
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.jdbc.JdbcDialects

object MaxComputeReader {
  def createBatchSource(
      sparkSession: SparkSession,
      source: MaxComputeSource,
      start: DateTime,
      end: DateTime
  ): DataFrame = {
    val maxComputeAccessID  = sys.env("CARAML_SPARK_MAXCOMPUTE_ACCESS_ID")
    val maxComputeAccessKey = sys.env("CARAML_SPARK_MAXCOMPUTE_ACCESS_KEY")
    val maxComputeJDBCConnectionURL =
      "jdbc:odps:https://service.ap-southeast-5.maxcompute.aliyun.com/api/?project=%s&interactiveMode=True&enableLimit=False" format source.project

    val sqlQuery = "(select * from `%s.%s`  where to_millis(%s) > %d and to_millis(%s) < %d)" format (
      source.dataset, source.table, source.eventTimestampColumn, start.getMillis, source.eventTimestampColumn, end.getMillis
    )

//    val sqlQuery = "(select * from `%s.%s`)" format (
//      source.dataset, source.table
//    )
    println("query is", sqlQuery)

    println(JdbcDialects.get("jdbc:odps:https://service.ap-southeast-5.maxcompute.aliyun.com/api/?project=%s&interactiveMode=True&enableLimit=False" format source.project))
    val customDialect = new CustomDialect()
    JdbcDialects.registerDialect(customDialect)
    println("custom dialect registered")
    println(JdbcDialects.get("jdbc:odps:https://service.ap-southeast-5.maxcompute.aliyun.com/api/?project=%s&interactiveMode=True&enableLimit=False" format source.project))

    val data = sparkSession.read
      .format("jdbc")
      .option("url", maxComputeJDBCConnectionURL)
      // Not setting queryTimeout will fail the query, whereas setting it up actually doesn't make an impact
      .option("queryTimeout", 5000)
      .option("dbtable", sqlQuery)
// ,option("query", sqlQuery)
      .option("user", maxComputeAccessID)
      .option("password", maxComputeAccessKey)
      .load()

    println(data)
    println(data.toDF().show(3))
    println(data.toDF().count())

//    data.toDF().registerTempTable("temp_table")
//    val valres = sparkSession.sql("select * from temp_table")
//
//    println("result from query", valres)
    data.toDF()
  }
}
