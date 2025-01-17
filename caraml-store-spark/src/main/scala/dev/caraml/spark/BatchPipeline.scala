package dev.caraml.spark

import dev.caraml.spark.metrics.IngestionPipelineMetrics
import dev.caraml.spark.sources.bq.BigQueryReader
import dev.caraml.spark.sources.file.FileReader
import dev.caraml.spark.validation.RowValidator
import dev.caraml.spark.results.KafkaResultStore
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Encoder, Row, SaveMode, SparkSession}

/**
  * Batch Ingestion Flow:
  * 1. Read from source (BQ | File)
  * 2. Map source columns to FeatureTable's schema
  * 3. Validate
  * 4. Store valid rows in redis
  * 5. Store invalid rows in parquet format at `deadletter` destination
  */
object BatchPipeline extends BasePipeline {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    val featureTable = config.featureTable
    val rowValidator =
      new RowValidator(featureTable, config.source.eventTimestampColumn, config.expectationSpec)
    val metrics = new IngestionPipelineMetrics
    val jobStartTime = System.currentTimeMillis()

    val input = config.source match {
      case source: BQSource =>
        BigQueryReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.bq,
          config.startTime,
          config.endTime
        )
      case source: FileSource =>
        FileReader.createBatchSource(
          sparkSession.sqlContext,
          source,
          config.startTime,
          config.endTime
        )
    }

    val projection =
      BasePipeline.inputProjection(
        config.source,
        featureTable.features,
        featureTable.entities,
        input.schema
      )

    val projected = if (config.deadLetterPath.nonEmpty) {
      input.select(projection: _*).cache()
    } else {
      input.select(projection: _*)
    }

    implicit val rowEncoder: Encoder[Row] = RowEncoder(projected.schema)

    val validRows = projected
      .map(metrics.incrementRead)
      .filter(rowValidator.allChecks)

    val onlineStore = config.store match {
      case _: RedisConfig    => "redis"
      case _: BigTableConfig => "bigtable"
      case _: HBaseConfig    => "hbase"
    }

    validRows.write
      .format(config.store match {
        case _: RedisConfig    => "dev.caraml.spark.stores.redis"
        case _: BigTableConfig => "dev.caraml.spark.stores.bigtable"
        case _: HBaseConfig    => "dev.caraml.spark.stores.bigtable"
      })
      .option("online_store", onlineStore)
      .option("entity_columns", featureTable.entities.map(_.name).mkString(","))
      .option("namespace", featureTable.name)
      .option("project_name", featureTable.project)
      .option("timestamp_column", config.source.eventTimestampColumn)
      .option("max_age", config.featureTable.maxAge.getOrElse(0L))
      .option("entity_max_age", config.entityMaxAge.getOrElse(0L))
      .save()

    val rowsWrittenCount = validRows.count() // Get the number of rows written to the store

    // Optionally, you can log or use the validRowCount and rowsWrittenCount variables as needed
    println(s"Number of rows written to the store: $rowsWrittenCount")

    config.deadLetterPath foreach { path =>
      projected
        .filter(!rowValidator.allChecks)
        .map(metrics.incrementDeadLetters)
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .save(StringUtils.stripEnd(path, "/") + "/" + SparkEnv.get.conf.getAppId)
    }

    val jobEndTime = System.currentTimeMillis()

    // perform result store if ResultStoreConfig is defined
    config.resultStore match {
      case Some(KafkaResultStoreConfig(bootstrapServers, topic)) =>
        val resultStore = new KafkaResultStore(bootstrapServers, topic)
        resultStore.storeResults(config, rowsWrittenCount)
      case None => // Do nothing
    }

    None
  }

}
