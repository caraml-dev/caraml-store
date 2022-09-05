package dev.caraml.spark.validation

import dev.caraml.spark.{ExpectationSpec, FeatureTable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

class RowValidator(
    featureTable: FeatureTable,
    timestampColumn: String,
    expectationSpec: Option[ExpectationSpec]
) extends Serializable {

  def allEntitiesPresent: Column =
    featureTable.entities.map(e => col(e.name).isNotNull).reduce(_.&&(_))

  def atLeastOneFeatureNotNull: Column =
    featureTable.features.map(f => col(f.name).isNotNull).reduce(_.||(_))

  def timestampPresent: Column =
    col(timestampColumn).isNotNull

  def validationChecks: Column = {

    expectationSpec match {
      case Some(value) if value.expectations.isEmpty => lit(true)
      case Some(value) =>
        value.expectations.map(_.validate).reduce(_.&&(_))
      case None => lit(true)
    }
  }

  def allChecks: Column =
    allEntitiesPresent && timestampPresent && validationChecks
}
