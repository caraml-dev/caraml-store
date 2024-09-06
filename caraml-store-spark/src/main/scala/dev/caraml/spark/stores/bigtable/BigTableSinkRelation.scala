package dev.caraml.spark.stores.bigtable

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import dev.caraml.spark.serialization.Serializer
import dev.caraml.spark.utils.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{
  Admin,
  ColumnFamilyDescriptorBuilder,
  Connection,
  Put,
  TableDescriptorBuilder
}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.BigTableSinkMetricSource
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, length, struct, udf}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StringType, StructType}

class BigTableSinkRelation(
    override val sqlContext: SQLContext,
    val serializer: Serializer,
    val config: SparkBigtableConfig,
    val hadoopConfig: Configuration
) extends BaseRelation
    with InsertableRelation
    with Serializable {

  import BigTableSinkRelation._

  override def schema: StructType = ???

  def getConnection(hadoopConfig: Configuration): Connection = {
    BigtableConfiguration.connect(hadoopConfig)
  }

  def createTable(): Unit = {
    val btConn = getConnection(hadoopConfig)
    try {
      val admin = btConn.getAdmin

      val table = if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
        val tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        val cf           = ColumnFamilyDescriptorBuilder.of(metadataColumnFamily)
        tableBuilder.setColumnFamily(cf)
        val table = tableBuilder.build()
        table
//        val t          = new HTableDescriptor(TableName.valueOf(tableName))
//        val metadataCF = new HColumnDescriptor(metadataColumnFamily)
//        t.addFamily(metadataCF)
//        t
      } else {
//        val t = admin.getTableDescriptor(TableName.valueOf(tableName))
        val t = btConn.getTable(TableName.valueOf(tableName))
        t.getDescriptor()
      }

//      val featuresCF = new HColumnDescriptor(config.namespace)
//      if (config.maxAge > 0) {
//        featuresCF.setTimeToLive(config.maxAge.toInt)
//      }
//      featuresCF.setMaxVersions(1)
      val featuresCFBuilder = ColumnFamilyDescriptorBuilder.newBuilder(config.namespace.getBytes)
      if (config.maxAge > 0) {
        featuresCFBuilder.setTimeToLive(config.maxAge.toInt)
      }
      featuresCFBuilder.setMaxVersions(1)
      val featuresCF = featuresCFBuilder.build()

      println("config.namespaces: ", config.namespace)
      val tdb = TableDescriptorBuilder.newBuilder(table)
      if (!table.getColumnFamilyNames.contains(config.namespace.getBytes)) {
//        table.addFamily(featuresCF)
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
      btConn.close()
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val jobConfig = new JobConf(hadoopConfig)
    val jobCreds  = jobConfig.getCredentials()
    UserGroupInformation.setConfiguration(data.sqlContext.sparkContext.hadoopConfiguration)
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())

    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))

    val featureColumns = featureFields.map(f => data(f.name))

    val sortedEntityColumns = config.entityColumns.sorted.map(c => data(c).cast(StringType))
    val schema              = serializer.convertSchema(StructType(featureFields))
    val schemaReference     = serializer.schemaReference(schema)

    data
      .select(
        joinEntityKey(struct(sortedEntityColumns: _*)).alias("key"),
        serializer.serializeData(schema)(struct(featureColumns: _*)).alias("value"),
        col(config.timestampColumn).alias("ts")
      )
      .where(length(col("key")) > 0)
      .rdd
      .map(convertToPut(config.namespace.getBytes, emptyQualifier.getBytes, schemaReference))
      .saveAsHadoopDataset(jobConfig)

  }

  def saveWriteSchema(data: DataFrame): Unit = {
    val featureFields = data.schema.fields
      .filterNot(f => isSystemColumn(f.name))
    val featureSchema = StructType(featureFields)

    val schema = serializer.convertSchema(featureSchema)
    val key    = schemaKeyPrefix.getBytes ++ serializer.schemaReference(schema)

    val put       = new Put(key)
    val qualifier = "avro".getBytes
    put.addColumn(metadataColumnFamily.getBytes, qualifier, schema.asInstanceOf[String].getBytes)

    val btConn = getConnection(hadoopConfig)
    try {
      val table = btConn.getTable(TableName.valueOf(tableName))
      table.checkAndPut(
        key,
        metadataColumnFamily.getBytes,
        qualifier,
        null,
        put
      )
    } finally {
      btConn.close()
    }
  }

  private def tableName: String = {
    val entities = config.entityColumns.sorted.mkString("__")
    StringUtils.trimAndHash(s"${config.projectName}__${entities}", maxTableNameLength)
  }

  private def joinEntityKey: UserDefinedFunction = udf { r: Row =>
    ((0 until r.size)).map(r.getString).mkString("#").getBytes
  }

  private val metadataColumnFamily = "metadata"
  private val schemaKeyPrefix      = "schema#"
  private val emptyQualifier       = ""
  private val maxTableNameLength   = 50

  private def isSystemColumn(name: String) =
    (config.entityColumns ++ Seq(config.timestampColumn)).contains(name)
}

object BigTableSinkRelation {
  def convertToPut(
      columnFamily: Array[Byte],
      column: Array[Byte],
      schemaReference: Array[Byte]
  ): Row => (Null, Put) =
    (r: Row) => {
      val put = new Put(r.getAs[Array[Byte]]("key"), r.getAs[java.sql.Timestamp]("ts").getTime)
      put.addColumn(
        columnFamily,
        column,
        schemaReference ++ r.getAs[Array[Byte]]("value")
      )

      metricSource.foreach(source => {
        val lag = System.currentTimeMillis() - r.getAs[java.sql.Timestamp]("ts").getTime
        source.METRIC_TOTAL_ROWS_INSERTED.inc()
        source.METRIC_ROWS_LAG.update(lag)
      })

      (null, put)
    }

  @transient
  lazy val metricSource: Option[BigTableSinkMetricSource] = {
    if (SparkEnv.get.metricsSystem.getSourcesByName(BigTableSinkMetricSource.sourceName).isEmpty) {
      SparkEnv.get.metricsSystem.registerSource(new BigTableSinkMetricSource)
    }

    SparkEnv.get.metricsSystem.getSourcesByName(BigTableSinkMetricSource.sourceName) match {
      case Seq(source: BigTableSinkMetricSource) => Some(source)
      case _                                     => None
    }
  }
}
