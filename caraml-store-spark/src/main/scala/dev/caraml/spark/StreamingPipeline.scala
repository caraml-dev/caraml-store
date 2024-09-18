package dev.caraml.spark

import dev.caraml.spark.metrics.{IngestionPipelineMetrics, StreamingMetrics}
import dev.caraml.spark.registry.ProtoRegistryFactory
import dev.caraml.spark.utils.ProtoReflection
import dev.caraml.spark.utils.testing.MemoryStreamingSource
import dev.caraml.spark.validation.RowValidator
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.SparkEnv

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

/**
  * Streaming pipeline (currently in micro-batches mode only, since we need to have multiple sinks: redis & deadletters).
  * Flow:
  * 1. Read from streaming source (currently only Kafka)
  * 2. Parse bytes from streaming source into Row with schema inferenced from provided class (Protobuf)
  * 3. Map columns according to provided mapping rules
  * 4. Validate
  * 5. (In batches) store to redis valid rows / write to deadletter (parquet) invalid
  */
object StreamingPipeline extends BasePipeline with Serializable {
  override def createPipeline(
      sparkSession: SparkSession,
      config: IngestionJobConfig
  ): Option[StreamingQuery] = {
    import sparkSession.implicits._

    val featureTable = config.featureTable
    val rowValidator =
      new RowValidator(featureTable, config.source.eventTimestampColumn, config.expectationSpec)
    val metrics          = new IngestionPipelineMetrics
    val streamingMetrics = new StreamingMetrics

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        streamingMetrics.updateStreamingProgress(event.progress)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    })

    val input = config.source match {
      case source: KafkaSource =>
        sparkSession.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", source.bootstrapServers)
          .option("subscribe", source.topic)
          .load()
      case source: MemoryStreamingSource =>
        source.read
    }

    val featureStruct = config.source.asInstanceOf[StreamingSource].format match {
      case ProtoFormat(classPath) =>
        val parser = protoParser(sparkSession, classPath)
        parser($"value")
      case AvroFormat(schemaJson) =>
        from_avro($"value", schemaJson)
      case _ =>
        val columns = input.columns.map(input(_))
        struct(columns: _*)
    }

    val metadata: Array[Column] = config.source match {
      case _: KafkaSource =>
        Array(col("timestamp"))
      case _ => Array()
    }

    val onlineStore = config.store match {
      case _: RedisConfig    => "redis"
      case _: BigTableConfig => "bigtable"
      case _: HBaseConfig    => "hbase"
    }

    val parsed = input
      .withColumn("features", featureStruct)
      .select(metadata :+ col("features.*"): _*)
    val projection =
      BasePipeline.inputProjection(
        config.source,
        featureTable.features,
        featureTable.entities,
        parsed.schema
      )

    val projected = parsed
      .select(projection ++ metadata: _*)

    val sink = projected.writeStream
      .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
        val rowsAfterValidation = batchDF.withColumn("_isValid", rowValidator.allChecks)
        rowsAfterValidation.persist()

        implicit val rowEncoder: Encoder[Row] = RowEncoder(rowsAfterValidation.schema)

        val metadataColName: Array[String] = metadata.map(_.toString)

        rowsAfterValidation
          .map(metrics.incrementRead)
          .filter(if (config.doNotIngestInvalidRows) expr("_isValid") else rowValidator.allChecks)
          .drop(metadataColName: _*)
          .write
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

        config.source match {
          case _: KafkaSource =>
            val timestamp: Option[Timestamp] = if (rowsAfterValidation.isEmpty) {
              None
            } else {
              Option(
                rowsAfterValidation
                  .agg(max("timestamp") as "latest_timestamp")
                  .collect()(0)
                  .getTimestamp(0)
              )
            }
            timestamp.foreach { t =>
              streamingMetrics.updateKafkaTimestamp(t.getTime)
            }
          case _ => ()
        }

        config.deadLetterPath match {
          case Some(path) =>
            rowsAfterValidation
              .filter("!_isValid")
              .map(metrics.incrementDeadLetters)
              .write
              .format("parquet")
              .mode(SaveMode.Append)
              .save(StringUtils.stripEnd(path, "/") + "/" + SparkEnv.get.conf.getAppId)
          case _ =>
            rowsAfterValidation
              .filter("!_isValid")
              .foreach(r => {
                println(s"Row failed validation $r")
              })
        }

        sparkSession.sharedState.cacheManager.uncacheQuery(batchDF, cascade = true)
        () // return Unit to avoid compile error with overloaded foreachBatch
      }

    val query = config.checkpointPath match {
      case Some(checkpointPath) =>
        sink
          .option(
            "checkpointLocation",
            StringUtils.stripEnd(checkpointPath, "/") + "/" + SparkEnv.get.conf.getAppId
          )
      case _ => sink
    }

    Some(
      query
        .trigger(ProcessingTimeTrigger.create(config.streamingTriggeringSecs, TimeUnit.SECONDS))
        .start()
    )
  }

  private def protoParser(sparkSession: SparkSession, className: String) = {
    val protoRegistry = ProtoRegistryFactory.resolveProtoRegistry(sparkSession)

    val parser: Array[Byte] => Row = ProtoReflection.createMessageParser(protoRegistry, className)

    // ToDo: create correctly typed parser
    // spark deprecated returnType argument, instead it will infer it from udf function signature
    udf(parser, ProtoReflection.inferSchema(protoRegistry.getProtoDescriptor(className)))
  }
}
