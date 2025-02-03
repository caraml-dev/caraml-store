package dev.caraml.spark.odps
import org.apache.spark.sql.jdbc.JdbcDialect

class CustomDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:odps")
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }
}