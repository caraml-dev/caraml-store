package dev.caraml.spark.validation

import dev.caraml.spark.FeatureTable
import dev.caraml.store.protobuf.types.ValueProto.ValueType
import org.apache.spark.sql.types._

object TypeCheck {
  def typesMatch(defType: ValueType.Enum, columnType: DataType): Boolean =
    (defType, columnType) match {
      case (ValueType.Enum.BOOL, BooleanType)                        => true
      case (ValueType.Enum.INT32, IntegerType)                       => true
      case (ValueType.Enum.INT64, LongType)                          => true
      case (ValueType.Enum.FLOAT, FloatType)                         => true
      case (ValueType.Enum.DOUBLE, DoubleType)                       => true
      case (ValueType.Enum.STRING, StringType)                       => true
      case (ValueType.Enum.BYTES, BinaryType)                        => true
      case (ValueType.Enum.BOOL_LIST, ArrayType(_: BooleanType, _))  => true
      case (ValueType.Enum.INT32_LIST, ArrayType(_: IntegerType, _)) => true
      case (ValueType.Enum.INT64_LIST, ArrayType(_: LongType, _))    => true
      case (ValueType.Enum.FLOAT_LIST, ArrayType(_: FloatType, _))   => true
      case (ValueType.Enum.DOUBLE_LIST, ArrayType(_: DoubleType, _)) => true
      case (ValueType.Enum.STRING_LIST, ArrayType(_: StringType, _)) => true
      case (ValueType.Enum.BYTES_LIST, ArrayType(_: BinaryType, _))  => true
      case _                                                         => false
    }

  /**
    * Verify whether types declared in FeatureTable match correspondent columns
    *
    * @param schema       Spark's dataframe columns
    * @param featureTable definition of expected schema
    * @return error details if some column doesn't match
    */
  def allTypesMatch(schema: StructType, featureTable: FeatureTable): Option[String] = {
    val typeByField =
      (featureTable.entities ++ featureTable.features).map(f => f.name -> f.`type`).toMap

    schema.fields
      .map(f =>
        if (typeByField.contains(f.name) && !typesMatch(typeByField(f.name), f.dataType)) {
          Some(
            s"Feature ${f.name} has different type ${f.dataType} instead of expected ${typeByField(f.name)}"
          )
        } else None
      )
      .foldLeft[Option[String]](None) {
        case (Some(error), _) => Some(error)
        case (_, Some(error)) => Some(error)
        case _                => None
      }
  }
}
