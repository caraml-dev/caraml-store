package dev.caraml.spark.stores.redis

import com.google.common.hash.Hashing
import com.google.protobuf.Timestamp
import dev.caraml.spark.utils.TypeConversion
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import redis.clients.jedis.Response
import redis.clients.jedis.commands.PipelineBinaryCommands

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._

/**
  * Use Redis hash type as storage layout. Every feature is stored as separate entry in Hash.
  * Also additional `timestamp` column is stored per FeatureTable to track update time.
  *
  * Keys are hashed as murmur3(`featureTableName` : `featureName`).
  * Values are serialized with protobuf (`ValueProto`).
  */
class HashTypePersistence(config: SparkRedisConfig) extends Persistence with Serializable {

  private def encodeRow(
      value: Row,
      expiryTimestamp: Option[java.sql.Timestamp]
  ): Map[Array[Byte], Array[Byte]] = {
    val fields = value.schema.fields.map(_.name)
    val types  = value.schema.fields.map(f => (f.name, f.dataType)).toMap
    val kvMap  = value.getValuesMap[Any](fields)

    val values = kvMap
      .filter { case (_, v) =>
        // don't store null values
        v != null
      }
      .filter { case (k, _) =>
        // don't store entities & timestamp
        !config.entityColumns.contains(k) && k != config.timestampColumn
      }
      .map { case (k, v) =>
        encodeKey(k) -> encodeValue(v, types(k))
      }

    val timestampHash = Seq(
      (
        timestampHashKey(config.namespace),
        encodeValue(value.getAs[Timestamp](config.timestampColumn), TimestampType)
      )
    )

    expiryTimestamp match {
      case Some(expiry) =>
        val expiryTimestampHash = Seq(
          (
            expiryTimestampHashKey(config.namespace),
            encodeValue(expiry, TimestampType)
          )
        )
        values ++ timestampHash ++ expiryTimestampHash
      case None => values ++ timestampHash
    }

  }

  private def encodeValue(value: Any, `type`: DataType): Array[Byte] = {
    TypeConversion.sqlTypeToProtoValue(value, `type`).toByteArray
  }

  private def encodeKey(key: String): Array[Byte] = {
    val fullFeatureReference = s"${config.namespace}:$key"
    Hashing.murmur3_32.hashString(fullFeatureReference, StandardCharsets.UTF_8).asBytes()
  }

  private def timestampHashKey(namespace: String): Array[Byte] = {
    Hashing.murmur3_32
      .hashString(s"${config.timestampPrefix}:${namespace}", StandardCharsets.UTF_8)
      .asBytes
  }

  private def expiryTimestampHashKey(namespace: String): Array[Byte] = {
    config.expiryPrefix.getBytes()
  }

  private def decodeTimestamp(encodedTimestamp: Array[Byte]): java.sql.Timestamp = {
    new java.sql.Timestamp(Timestamp.parseFrom(encodedTimestamp).getSeconds * 1000)
  }

  override def save(
      pipeline: PipelineBinaryCommands,
      key: Array[Byte],
      row: Row,
      expiryTimestamp: Option[java.sql.Timestamp]
  ): Unit = {
    val value = encodeRow(row, expiryTimestamp).asJava
    pipeline.hset(key, value)

    expiryTimestamp match {
      case Some(expiry) =>
        pipeline.expireAt(key, expiry.getTime / 1000)
      case None =>
        pipeline.persist(key)
    }
  }

  override def get(
      pipeline: PipelineBinaryCommands,
      key: Array[Byte]
  ): Response[util.Map[Array[Byte], Array[Byte]]] = {
    pipeline.hgetAll(key)
  }

  override def storedTimestamp(
      value: util.Map[Array[Byte], Array[Byte]]
  ): Option[java.sql.Timestamp] = {
    value.asScala.toMap
      .map { case (key, value) =>
        (wrapByteArray(key), value)
      }
      .get(timestampHashKey(config.namespace))
      .map(value => decodeTimestamp(value))
  }
}
