package dev.caraml.spark.odps
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.ajax.JSON

class CustomDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:odps")
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
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
