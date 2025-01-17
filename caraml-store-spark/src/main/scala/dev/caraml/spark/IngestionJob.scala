package dev.caraml.spark

import dev.caraml.spark.utils.JsonUtils
import dev.caraml.store.protobuf.types.ValueProto.ValueType
import org.apache.log4j.Logger
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s._
import org.json4s.ext.JavaEnumNameSerializer
import org.json4s.jackson.JsonMethods.{parse => parseJSON}

object IngestionJob {
  import Modes._
  implicit val modesRead: scopt.Read[Modes.Value] =
    scopt.Read.reads(Modes withName _.capitalize)
  implicit val formats: Formats = DefaultFormats +
    new JavaEnumNameSerializer[ValueType.Enum]() +
    ShortTypeHints(List(classOf[ProtoFormat], classOf[AvroFormat]))

  private val logger = Logger.getLogger(getClass.getCanonicalName)

  val parser = new scopt.OptionParser[IngestionJobConfig]("IngestionJob") {
    // ToDo: read version from Manifest
    head("dev.caraml.spark.IngestionJob", "latest")

    opt[Modes]("mode")
      .action((x, c) => c.copy(mode = x))
      .required()
      .text("Mode to operate ingestion job (offline or online)")

    opt[String](name = "source")
      .action((x, c) => {
        val json = parseJSON(x)
        JsonUtils
          .mapFieldWithParent(json) {
            case (parent: String, (key: String, v: JValue))
                if !parent.equals("fieldMapping") =>
              JsonUtils.camelize(key) -> v
            case (_, x) => x
          }
          .extract[Sources] match {
          case Sources(file: Some[FileSource], _, _) =>
            c.copy(source = file.get)
          case Sources(_, bq: Some[BQSource], _) => c.copy(source = bq.get)
          case Sources(_, _, kafka: Some[KafkaSource]) =>
            c.copy(source = kafka.get)
        }
      })
      .required()
      .text(
        """JSON-encoded source object (e.g. {"kafka":{"bootstrapServers":...}}"""
      )

    opt[String](name = "feature-table")
      .action((x, c) => {
        val ft = parseJSON(x).camelizeKeys.extract[FeatureTable]

        c.copy(
          featureTable = ft,
          streamingTriggeringSecs =
            ft.labels.getOrElse("_streaming_trigger_secs", "0").toInt,
          validationConfig = ft.labels
            .get("_validation")
            .map(parseJSON(_).camelizeKeys.extract[ValidationConfig]),
          expectationSpec = ft.labels
            .get("_expectations")
            .map(parseJSON(_).camelizeKeys.extract[ExpectationSpec])
        )
      })
      .required()
      .text("JSON-encoded FeatureTableSpec object")

    opt[String](name = "start")
      .action((x, c) => c.copy(startTime = DateTime.parse(x)))
      .text("Start timestamp for offline ingestion")

    opt[String](name = "end")
      .action((x, c) => c.copy(endTime = DateTime.parse(x)))
      .text("End timestamp for offline ingestion")

    opt[String](name = "entity-max-age")
      .action((x, c) => c.copy(entityMaxAge = Some(x.toLong)))
      .text(
        "Maximum max age for all the feature table sharing the same entities"
      )

    opt[String](name = "ingestion-timespan")
      .action((x, c) => {
        val currentTimeUTC = new DateTime(DateTimeZone.UTC);
        val startTime =
          currentTimeUTC.withTimeAtStartOfDay().minusDays(x.toInt - 1)
        val endTime = currentTimeUTC.withTimeAtStartOfDay().plusDays(1)
        c.copy(startTime = startTime, endTime = endTime)
      })
      .text("Ingestion timespan")

    opt[String](name = "redis")
      .action((x, c) => c.copy(store = parseJSON(x).extract[RedisConfig]))

    opt[String](name = "bigtable")
      .action((x, c) =>
        c.copy(store = parseJSON(x).camelizeKeys.extract[BigTableConfig])
      )

    opt[String](name = "hbase")
      .action((x, c) => c.copy(store = parseJSON(x).extract[HBaseConfig]))

    opt[String](name = "statsd")
      .action((x, c) =>
        c.copy(metrics = Some(parseJSON(x).extract[StatsDConfig]))
      )

    opt[String](name = "deadletter-path")
      .action((x, c) => c.copy(deadLetterPath = Some(x)))

    opt[String](name = "stencil-url")
      .action((x, c) => c.copy(stencilURL = Some(x)))

    opt[String](name = "stencil-token")
      .action((x, c) => c.copy(stencilToken = Some(x)))

    opt[Unit](name = "drop-invalid")
      .action((_, c) => c.copy(doNotIngestInvalidRows = true))

    opt[String](name = "checkpoint-path")
      .action((x, c) => c.copy(checkpointPath = Some(x)))

    opt[Int](name = "triggering-interval")
      .action((x, c) => c.copy(streamingTriggeringSecs = x))

    opt[String](name = "bq")
      .action((x, c) => c.copy(bq = Some(parseJSON(x).extract[BQConfig])))

    opt[String](name = "result-store")
      .action((x, c) => {
        parseJSON(x).extract[ResultStore] match {
          case ResultStore(kafka: Some[KafkaResultStoreConfig]) =>
            c.copy(resultStore = Some(kafka.get))
          case _ => c
        }
      })

  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, IngestionJobConfig()) match {
      case Some(config) =>
        println(s"Starting with config $config")
        config.mode match {
          case Modes.Offline =>
            val sparkSession = BasePipeline.createSparkSession(config)
            try {
              BatchPipeline.createPipeline(sparkSession, config)
            } catch {
              case e: Throwable =>
                logger.fatal("Batch ingestion failed", e)
                throw e
            } finally {
              sparkSession.close()
            }
          case Modes.Online =>
            val sparkSession = BasePipeline.createSparkSession(config)
            try {
              StreamingPipeline
                .createPipeline(sparkSession, config)
                .get
                .awaitTermination
            } catch {
              case e: Throwable =>
                logger.fatal("Streaming ingestion failed", e)
                throw e
            } finally {
              sparkSession.close()
            }
        }
      case None =>
        println("Parameters can't be parsed")
    }
  }

}
