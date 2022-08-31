package dev.caraml.spark.stores.redis

import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.Try

object PipelineProviderFactory {

  private lazy val providers: mutable.Map[RedisEndpoint, PipelineProvider] = mutable.Map.empty

  private def newJedisClient(endpoint: RedisEndpoint): Jedis = {
    val jedis = new Jedis(endpoint.host, endpoint.port)
    if (endpoint.password.nonEmpty) {
      jedis.auth(endpoint.password)
    }
    jedis
  }

  private def checkIfInClusterMode(endpoint: RedisEndpoint): Boolean = {
    val jedis     = newJedisClient(endpoint)
    val isCluster = Try(jedis.clusterInfo()).isSuccess
    jedis.close()
    isCluster
  }

  private def clusterPipelineProvider(endpoint: RedisEndpoint): PipelineProvider = {
    ClusterPipelineProvider(endpoint)
  }

  private def singleNodePipelineProvider(endpoint: RedisEndpoint): PipelineProvider = {
    SingleNodePipelineProvider(endpoint)
  }

  def newProvider(endpoint: RedisEndpoint): PipelineProvider = {
    if (checkIfInClusterMode(endpoint)) {
      clusterPipelineProvider(endpoint)
    } else {
      singleNodePipelineProvider(endpoint)
    }
  }

  def provider(endpoint: RedisEndpoint): PipelineProvider = {
    providers.getOrElseUpdate(endpoint, newProvider(endpoint))
  }
}
