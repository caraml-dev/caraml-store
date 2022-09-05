package dev.caraml.spark.validation

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue}

trait Expectation {

  def validate: Column
}

case class ExpectColumnValuesToNotBeNull(columnName: String) extends Expectation {
  override def validate: Column = col(columnName).isNotNull
}

case class ExpectColumnValuesToBeBetween(
    columnName: String,
    minValue: Option[Int],
    maxValue: Option[Int]
) extends Expectation {
  override def validate: Column = {
    (minValue, maxValue) match {
      case (Some(min), Some(max)) => col(columnName).between(min, max)
      case (Some(min), None)      => col(columnName).>=(min)
      case (None, Some(max))      => col(columnName).<=(max)
      case _                      => lit(true)
    }
  }
}

object Expectation {
  implicit val format: Formats = DefaultFormats

  def extractColumn(kwargs: JValue): String = {
    (kwargs \ "column").extract[String]
  }

  def apply(expectationType: String, kwargs: JValue): Expectation = {
    expectationType match {
      case "expect_column_values_to_not_be_null" =>
        ExpectColumnValuesToNotBeNull(extractColumn(kwargs))
      case "expect_column_values_to_be_between" =>
        val column   = extractColumn(kwargs)
        val minValue = (kwargs \ "minValue").toSome.map(_.extract[Int])
        val maxValue = (kwargs \ "maxValue").toSome.map(_.extract[Int])
        ExpectColumnValuesToBeBetween(column, minValue, maxValue)
    }
  }
}

object ExpectationCodec
    extends CustomSerializer[Expectation](implicit format =>
      (
        { case x: JObject =>
          val eType: String  = (x \ "expectationType").extract[String]
          val kwargs: JValue = (x \ "kwargs")
          Expectation(eType, kwargs)
        },
        { case x: Expectation =>
          Extraction.decompose(x)
        }
      )
    )
