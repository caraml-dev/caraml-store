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
    * @param expiryTimestamp       Expiry timestamp for the row
    * @param maxExpiryTimestamp    No ttl should be set if the expiry timestamp
    *                              is equal to the maxExpiryTimestamp
    */
  def save(
      pipeline: PipelineBinaryCommands,
      key: Array[Byte],
      row: Row,
      expiryTimestamp: Option[Timestamp]
  ): Unit

  /**
    * Returns a Redis response, which can be used by `storedTimestamp` and `newExpiryTimestamp` to
    * derive the currently stored event timestamp, and the updated expiry timestamp. This method will
    * be called prior to persisting the row to Redis, so that `RedisSinkRelation` can decide whether
    * the currently stored value should be updated.
    *
    * @param pipeline              Redis pipeline
    * @param key                   Redis key in serialized bytes format
    * @return                      Redis response representing the row value
    */
  def get(
      pipeline: PipelineBinaryCommands,
      key: Array[Byte]
  ): Response[util.Map[Array[Byte], Array[Byte]]]

  /**
    * Returns the currently stored event timestamp for the key and the feature table associated with the ingestion job.
    *
    * @param value              Response returned from `get`
    * @return                   Stored event timestamp associated with the key. Returns `None` if
    *                           the key is not present in Redis, or if timestamp information is
    *                           unavailable on the stored value.
    */
  def storedTimestamp(value: util.Map[Array[Byte], Array[Byte]]): Option[Timestamp]

}
