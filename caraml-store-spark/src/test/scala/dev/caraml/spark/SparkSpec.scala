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

    sparkSession = SparkSession
      .builder()
      .config(withSparkConfOverrides(sparkConf))
      .getOrCreate()
  }

  after {
    sparkSession.stop()
  }
}
