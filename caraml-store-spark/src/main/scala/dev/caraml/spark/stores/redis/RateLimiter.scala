package dev.caraml.spark.stores.redis

import dev.caraml.spark.RedisWriteProperties
import io.github.bucket4j.{Bandwidth, Bucket}

import java.time.Duration.ofSeconds
import java.util.concurrent.ConcurrentHashMap

object RateLimiter {

  private lazy val buckets: ConcurrentHashMap[RedisWriteProperties, Bucket] = new ConcurrentHashMap

  def get(properties: RedisWriteProperties): Bucket = {
    buckets.computeIfAbsent(properties, create)
  }

  def create(properties: RedisWriteProperties): Bucket = {
    Bucket
      .builder()
      .addLimit(
        Bandwidth
          .builder()
          .capacity(properties.ratePerSecondLimit)
          .refillIntervally(properties.ratePerSecondLimit, ofSeconds(1))
          .build()
      )
      .build()
  }
}
