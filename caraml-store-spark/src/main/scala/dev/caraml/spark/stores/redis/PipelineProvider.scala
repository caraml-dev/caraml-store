package dev.caraml.spark.stores.redis

import redis.clients.jedis.commands.PipelineBinaryCommands

/**
  * Provide either a pipeline or cluster pipeline to read and write data into Redis.
  */
trait PipelineProvider {

  def withPipeline[T](ops: PipelineBinaryCommands => T): T

  /**
    * Close client connection
    */
  def close(): Unit
}
