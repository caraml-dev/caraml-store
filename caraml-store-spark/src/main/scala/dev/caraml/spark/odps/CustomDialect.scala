package dev.caraml.spark.odps
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import java.sql.SQLException

class CustomDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:odps")
  }

  override def quoteIdentifier(colName: String): String = {
    s"$colName"
  }

  /*
    TODO: currently unsupported types
    - ARRAY<DECIMAL(precision,scale)>
    - ARRAY<VARCHAR(n)>    --> temporarily map it as a string
    - ARRAY<CHAR(n)>    --> temporarily map it as a string
    - ARRAY<DATE>
    - ARRAY<DATETIME>
    - ARRAY<TIMESTAMP>
    - ARRAY<TIMESTAMP_NTZ>
    - ARRAY<INTERVAL>

    typeName below were obtained from https://www.alibabacloud.com/help/en/maxcompute/user-guide/maxcompute-v2-0-data-type-edition
   */
  private def getCommonCatalystType(typeName: String): Option[DataType] = {
    typeName.toUpperCase() match {
      case "TINYINT"  => Option(ByteType)
      case "SMALLINT" => Option(ShortType)
      case "INT"      => Option(IntegerType)
      case "BIGINT"   => Option(LongType)
      case "BINARY"   => Option(BinaryType)
      case "FLOAT"    => Option(FloatType)
      case "DOUBLE"   => Option(DoubleType)
//      case s if s.startsWith("DECIMAL") =>
//        val mdat = s.stripPrefix("DECIMAL(").stripSuffix(")").split(",")
//        if (mdat.length == 2) {
//          val precision = mdat(0).toInt
//          val scale = mdat(1).toInt
//          Option(DecimalType(min(precision, DecimalType.MAX_PRECISION), min(scale, DecimalType.MAX_SCALE)))
//        } else {
//          Option(DecimalType.SYSTEM_DEFAULT)
//        }
//      case s if s.startsWith("VARCHAR") => Option(VarcharType(s.stripPrefix("VARCHAR(").stripSuffix(")").toInt))
//      case s if s.startsWith("CHAR") => Option(CharType(s.stripPrefix("CHAR(").stripSuffix(")").toInt))
      case s if s.startsWith("VARCHAR") => Option(StringType)
      case s if s.startsWith("CHAR")    => Option(StringType)
      case "STRING"                     => Option(StringType)
//      case "DATE" => Option(DateType)
//      case "DATETIME" => Option(TimestampType)
//      case "TIMESTAMP" => Option(TimestampType)
//      case "TIMESTAMP_NTZ" => Option(TimestampType)
      case "BOOLEAN" => Option(BooleanType)
//      case "INTERVAL" => Option(CalendarIntervalType)
      case _ => None
    }
  }

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder
  ): Option[DataType] = {
    sqlType match {
      case java.sql.Types.ARRAY =>
        val elementTypeName = typeName.toUpperCase().stripPrefix("ARRAY<").stripSuffix(">")
        val elementType     = getCommonCatalystType(elementTypeName).map(ArrayType(_))

        if (elementType.isEmpty) {
          throw new SQLException(s"Unsupported type $typeName")
        }
        logDebug(
          s"CustomDialect sqlType: $sqlType md: ${md.build().toString()} size: $size typeName: $typeName elementType: ${elementType.getOrElse(ArrayType(NullType)).elementType}"
        )
        elementType
      case _ =>
        val dataType = getCommonCatalystType(typeName.toUpperCase())
        logDebug(
          s"CustomDialect sqlType: $sqlType md: ${md.build().toString()} size: $size typeName: $typeName dataType: $dataType"
        )
        dataType
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType    => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType =>
        Option(JdbcType("DOUBLE", java.sql.Types.DOUBLE))
      case FloatType   => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
      case ShortType   => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
      case ByteType    => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
      case StringType  => Option(JdbcType("STRING", java.sql.Types.CLOB))
      case BinaryType  => Option(JdbcType("BINARY", java.sql.Types.BINARY))
      case TimestampType =>
        Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType =>
        Option(
          JdbcType(
            s"DECIMAL(${t.precision},${t.scale})",
            java.sql.Types.DECIMAL
          )
        )
      case VarcharType(length) => Option(JdbcType(s"VARCHAR($length)", java.sql.Types.VARCHAR))
      // NOTE: Update when necessary
      case _ => None
    }
  }
}
