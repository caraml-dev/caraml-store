package dev.caraml.spark.stores.redis

import dev.caraml.spark.RedisWriteProperties
import dev.caraml.spark.utils.TypeConversion
import io.github.bucket4j.{Bandwidth, Bucket}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.metrics.source.RedisSinkMetricSource
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkEnv}

import java.time.Duration.ofSeconds

/**
  * High-level writer to Redis. Relies on `Persistence` implementation for actual storage layout.
  * Here we define general flow:
  *
  * 1. Deduplicate rows within one batch (group by key and get only latest (by timestamp))
  * 2. Read last-stored timestamp from Redis
  * 3. Check if current timestamp is more recent than already saved one
  * 4. Save to storage if it's the case
  */
class RedisSinkRelation(override val sqlContext: SQLContext, config: SparkRedisConfig)
    extends BaseRelation
    with InsertableRelation
    with Serializable {

  import RedisSinkRelation._

  override def schema: StructType = ???

  val persistence: Persistence = new HashTypePersistence(config)

  val sparkConf: SparkConf = sqlContext.sparkContext.getConf

  lazy val endpoint: RedisEndpoint = RedisEndpoint(
    host = sparkConf.get("spark.redis.host"),
    port = sparkConf.get("spark.redis.port").toInt,
    password = sparkConf.get("spark.redis.password", "")
  )

  lazy val properties: RedisWriteProperties = RedisWriteProperties(
    maxJitterSeconds = sparkConf.get("spark.redis.properties.maxJitter").toInt,
    pipelineSize = sparkConf.get("spark.redis.properties.pipelineSize").toInt,
    ttlSeconds = config.entityMaxAge,
    enableRateLimit = sparkConf.get("spark.redis.properties.enableRateLimit").toBoolean,
    ratePerSecondLimit = sparkConf.get("spark.redis.properties.ratePerSecondLimit").toInt
  )

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.foreachPartition { partition: Iterator[Row] =>
      java.security.Security.setProperty("networkaddress.cache.ttl", "3");
      java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");

      val pipelineProvider = PipelineProviderFactory.provider(endpoint)

      // grouped iterator to only allocate memory for a portion of rows
      partition.grouped(properties.pipelineSize).foreach { batch =>
        if (properties.enableRateLimit) {
          val rateLimitBucket = RateLimiter.get(properties)
          rateLimitBucket.asBlocking().consume(batch.length)
        }
        val rowsWithKey: Seq[(String, Row)] = batch.map(row => dataKeyId(row) -> row)

        pipelineProvider.withPipeline(pipeline => {
          rowsWithKey.foreach { case (key, row) =>
            if (metricSource.nonEmpty) {
              val lag = System.currentTimeMillis() - row
                .getAs[java.sql.Timestamp](config.timestampColumn)
                .getTime

              metricSource.get.METRIC_TOTAL_ROWS_INSERTED.inc()
              metricSource.get.METRIC_ROWS_LAG.update(lag)
            }
            persistence.save(
              pipeline,
              key.getBytes(),
              row,
              properties.ttlSeconds,
              properties.maxJitterSeconds
            )
          }
        })
      }
    }
  }

  /**
    * Key is built from entities columns values with prefix of entities columns names.
    */
  private def dataKeyId(row: Row): String = {
    val types = row.schema.fields.map(f => (f.name, f.dataType)).toMap

    val sortedEntities = config.entityColumns.sorted.toSeq
    val entityValues = sortedEntities
      .map(entity => (row.getAs[Any](entity), types(entity)))
      .map { case (value, v_type) =>
        TypeConversion.sqlTypeToString(value, v_type)
      }
    DigestUtils.md5Hex(
      s"${config.projectName}#${sortedEntities.mkString("#")}:${entityValues.mkString("#")}"
    )
  }

  private lazy val metricSource: Option[RedisSinkMetricSource] = {
    MetricInitializationLock.synchronized {
      // RedisSinkMetricSource needs to be registered on executor and SparkEnv must already exist.
      // Which is problematic, since metrics system is initialized before SparkEnv set.
      // That's why I moved source registering here
      if (SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName).isEmpty) {
        SparkEnv.get.metricsSystem.registerSource(new RedisSinkMetricSource)
      }
    }

    SparkEnv.get.metricsSystem.getSourcesByName(RedisSinkMetricSource.sourceName) match {
      case Seq(source: RedisSinkMetricSource) => Some(source)
      case _                                  => None
    }
  }
}

object RedisSinkRelation {
  object MetricInitializationLock
}
