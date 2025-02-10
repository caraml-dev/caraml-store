package dev.caraml.spark.sources.maxCompute

import org.apache.spark.sql.jdbc.JdbcDialect

class CustomDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = {
        url.startsWith("jdbc:odps")
    }

    override def quoteIdentifier(colName: String): String = {
        s"$colName"
    }

    override def getSchemaQuery(table: String): String = {
        table
    }
}