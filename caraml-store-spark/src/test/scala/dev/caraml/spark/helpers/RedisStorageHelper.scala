package dev.caraml.spark.helpers

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import com.google.protobuf.Timestamp
import dev.caraml.spark.utils.TypeConversion._
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.contain
import com.google.common.hash.Hashing
import dev.caraml.spark.FeatureTable
import dev.caraml.store.protobuf.types.ValueProto

import scala.util.Try

object RedisStorageHelper {
  def encodeFeatureKey(featureTable: FeatureTable)(feature: String): String = {
    val fullReference = s"${featureTable.name}:$feature"
    murmurHashHexString(fullReference)
  }

  def murmurHashHexString(s: String): String = {
    Hashing.murmur3_32().hashString(s, StandardCharsets.UTF_8).asInt().toHexString
  }

  def beStoredRow(mappedRow: Map[String, Any]): Matcher[Map[Array[Byte], Array[Byte]]] = {
    val m: Matcher[Map[String, Any]] = contain.allElementsOf(mappedRow).matcher

    m compose {
      (_: Map[Array[Byte], Array[Byte]])
        .map {
          case (k, v) if k.sameElements("_ex".getBytes()) =>
            (new String(k), Timestamp.parseFrom(v).asScala)

          case (k, v) if k.length == 4 =>
            (
              ByteBuffer.wrap(k).order(ByteOrder.LITTLE_ENDIAN).getInt.toHexString,
              Try(ValueProto.Value.parseFrom(v).asScala).getOrElse(Timestamp.parseFrom(v).asScala)
            )
        }
    }
  }
}
