package dev.caraml.spark.stores.bigtable

import dev.caraml.spark.serialization.Serializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
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
}