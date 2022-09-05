package dev.caraml.spark.stores.redis

import redis.clients.jedis.JedisPool
import redis.clients.jedis.commands.PipelineBinaryCommands

/**
  * Provide pipeline for single node Redis.
  */
case class SingleNodePipelineProvider(endpoint: RedisEndpoint) extends PipelineProvider {

  val jedisPool = new JedisPool(endpoint.host, endpoint.port)

  /**
    * @return execute command within a pipeline and return the result
    */
  override def withPipeline[T](ops: PipelineBinaryCommands => T): T = {
    val jedis = jedisPool.getResource
    if (endpoint.password.nonEmpty) {
      jedis.auth(endpoint.password)
    }
    val response = ops(jedis.pipelined())
    jedis.close()
    response
  }

  /**
    * Close client connection
    */
  override def close(): Unit = jedisPool.close()

}
