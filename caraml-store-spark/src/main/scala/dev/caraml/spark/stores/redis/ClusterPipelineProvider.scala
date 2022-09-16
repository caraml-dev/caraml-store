package dev.caraml.spark.stores.redis

import redis.clients.jedis.commands.PipelineBinaryCommands
import redis.clients.jedis.providers.ClusterConnectionProvider
import redis.clients.jedis.{ClusterPipeline, DefaultJedisClientConfig, HostAndPort}

import scala.collection.JavaConverters._

/**
  * Provide pipeline for Redis cluster.
  */
case class ClusterPipelineProvider(endpoint: RedisEndpoint) extends PipelineProvider {

  val nodes = Set(new HostAndPort(endpoint.host, endpoint.port)).asJava
  val configBuilder = DefaultJedisClientConfig
    .builder()
  val DEFAULT_CLIENT_CONFIG =
    if (endpoint.password.isEmpty) configBuilder.build()
    else configBuilder.password(endpoint.password).build()
  val provider = new ClusterConnectionProvider(nodes, DEFAULT_CLIENT_CONFIG)

  /**
    * @return execute commands within a pipeline and return the result
    */
  override def withPipeline[T](ops: PipelineBinaryCommands => T): T = {
    val pipeline = new ClusterPipeline(provider)
    val response = ops(pipeline)
    pipeline.close()
    response
  }

  /**
    * Close client connection
    */
  override def close(): Unit = {
    provider.close()
  }
}
