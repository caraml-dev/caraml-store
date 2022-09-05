package dev.caraml.spark.stores.redis

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Entrypoint to Redis Storage. Implements only `CreatableRelationProvider` since it's only possible write to Redis.
  * Here we parse configuration from spark parameters & provide SparkRedisConfig to `RedisSinkRelation`
  */
class RedisRelationProvider extends CreatableRelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val config   = SparkRedisConfig.parse(parameters)
    val relation = new RedisSinkRelation(sqlContext, config)

    relation.insert(data, overwrite = false)

    relation
  }
}

class DefaultSource extends RedisRelationProvider
