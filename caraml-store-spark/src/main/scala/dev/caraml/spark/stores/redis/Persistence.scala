package dev.caraml.spark.stores.redis

import org.apache.spark.sql.Row
import redis.clients.jedis.Response
import redis.clients.jedis.commands.PipelineBinaryCommands

import java.sql.Timestamp
import java.util

/**
  * Determine how a Spark row should be serialized and stored on Redis.
  */
trait Persistence {

  /**
    * Persist a Spark row to Redis
    *
    * @param pipeline              Redis pipeline
    * @param key                   Redis key in serialized bytes format
    * @param row                   Row representing the value to be persist
    * @param ttlSeconds            TTL seconds
    */
  def save(
      pipeline: PipelineBinaryCommands,
      key: Array[Byte],
      row: Row,
      ttlSeconds: Long,
      maxJitterSeconds: Int
  ): Unit
}
