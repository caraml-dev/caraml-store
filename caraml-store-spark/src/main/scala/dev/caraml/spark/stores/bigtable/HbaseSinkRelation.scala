package dev.caraml.spark.stores.bigtable

import dev.caraml.spark.serialization.Serializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{
  ColumnFamilyDescriptorBuilder,
  Connection,
  ConnectionFactory,
  TableDescriptorBuilder
}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.spark.sql.SQLContext

class HbaseSinkRelation(
    sqlContext: SQLContext,
    serializer: Serializer,
    config: SparkBigtableConfig,
    hadoopConfig: Configuration
) extends BigTableSinkRelation(sqlContext, serializer, config, hadoopConfig) {
  override def getConnection(hadoopConfig: Configuration): Connection = {
    ConnectionFactory.createConnection(hadoopConfig)
  }
  override def createTable(): Unit = {
    val hbaseConn = getConnection(hadoopConfig)
    try {
      val admin = hbaseConn.getAdmin

      val table = if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
        val tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        val cf           = ColumnFamilyDescriptorBuilder.of(metadataColumnFamily)
        tableBuilder.setColumnFamily(cf)
        val table = tableBuilder.build()
        table
      } else {
        val t = hbaseConn.getTable(TableName.valueOf(tableName))
        t.getDescriptor()
      }
      val featuresCFBuilder = ColumnFamilyDescriptorBuilder.newBuilder(config.namespace.getBytes)
      if (config.maxAge > 0) {
        featuresCFBuilder.setTimeToLive(config.maxAge.toInt)
      }
      featuresCFBuilder.setMaxVersions(1)
      featuresCFBuilder.setCompressionType(Compression.Algorithm.ZSTD)
      val featuresCF = featuresCFBuilder.build()

      val tdb = TableDescriptorBuilder.newBuilder(table)
      // TODO: make this configurable
      tdb.setRegionSplitPolicyClassName(
        "org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy"
      )

      if (!table.getColumnFamilyNames.contains(config.namespace.getBytes)) {
        tdb.setColumnFamily(featuresCF)
        val t = tdb.build()
        if (!admin.isTableAvailable(table.getTableName)) {
          admin.createTable(t)
        } else {
          admin.modifyTable(t)
        }
      } else if (
        config.maxAge > 0 && table
          .getColumnFamily(config.namespace.getBytes)
          .getTimeToLive != featuresCF.getTimeToLive
      ) {
        tdb.modifyColumnFamily(featuresCF)
        admin.modifyTable(tdb.build())
      }
    } finally {
      hbaseConn.close()
    }
  }
}
