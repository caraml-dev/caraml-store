package dev.caraml.spark.sources.maxCompute

import org.apache.spark.sql.jdbc.JdbcDialect

class CustomDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = {
        println("can handle? this one")
        println(url.startsWith("jdbc:odps"), url)
        url.startsWith("jdbc:odps")
    }

    override def quoteIdentifier(colName: String): String = {
        println("inside quote identifier", colName, s"$colName")
        s"$colName"
    }

    override def getSchemaQuery(table: String): String = {
        println("getschemaquery", table)
        table
    }
}