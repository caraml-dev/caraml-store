package dev.caraml.spark.helpers

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.joda.time.{DateTime, Seconds}
import org.scalacheck.Gen

import scala.reflect.runtime.universe.TypeTag

case class TestRow(
    customer: String,
    feature1: Int,
    feature2: Float,
    eventTimestamp: java.sql.Timestamp
)

object DataHelper {
  def generateRows[A](gen: Gen[A], N: Int): Seq[A] =
    Gen.listOfN(N, gen).sample.get

  def generateDistinctRows[A](gen: Gen[A], N: Int, entityFun: A => String): Seq[A] =
    generateRows(gen, N).groupBy(entityFun).map(_._2.head).toSeq

  def generateTempPath(last: String): String =
    Paths.get(Files.createTempDirectory("test-dir").toString, last).toString

  def storeAsParquet[T <: Product: TypeTag](sparkSession: SparkSession, rows: Seq[T]): String = {
    import sparkSession.implicits._

    val tempPath = generateTempPath("rows")

    sparkSession
      .createDataset(rows)
      .withColumn("date", to_date($"eventTimestamp"))
      .write
      .partitionBy("date")
      .save(tempPath)

    tempPath
  }

  def rowGenerator(
      start: DateTime,
      end: DateTime,
      customerGen: Option[Gen[String]] = None
  ): Gen[TestRow] =
    for {
      customer <- customerGen.getOrElse(Gen.asciiPrintableStr)
      feature1 <- Gen.choose(0, 100)
      feature2 <- Gen.choose[Float](0, 1)
      eventTimestamp <- Gen
        .choose(0, Seconds.secondsBetween(start, end).getSeconds - 1)
        .map(start.withMillisOfSecond(0).plusSeconds)
    } yield TestRow(
      customer,
      feature1,
      feature2,
      new java.sql.Timestamp(eventTimestamp.getMillis)
    )
}
