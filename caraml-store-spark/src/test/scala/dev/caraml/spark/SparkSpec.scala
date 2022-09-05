package dev.caraml.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter

class SparkSpec extends UnitSpec with BeforeAndAfter {
  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  var sparkSession: SparkSession                         = null
  def withSparkConfOverrides(conf: SparkConf): SparkConf = conf

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Testing")
      .set("spark.driver.bindAddress", "localhost")
      .set("spark.default.parallelism", "8")
      .set(
        "spark.metrics.conf.*.sink.statsd.class",
        "org.apache.spark.metrics.sink.StatsdSinkWithTags"
      )
      .set("spark.metrics.conf.*.sink.statsd.host", "localhost")
      .set("spark.metrics.conf.*.sink.statsd.period", "999") // disable scheduled reporting
      .set("spark.metrics.conf.*.sink.statsd.unit", "minutes")
      .set("spark.metrics.labels", "job_id=test")
      .set("spark.metrics.namespace", "")
      .set("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")

    sparkSession = SparkSession
      .builder()
      .config(withSparkConfOverrides(sparkConf))
      .getOrCreate()
  }

  after {
    sparkSession.stop()
  }
}
