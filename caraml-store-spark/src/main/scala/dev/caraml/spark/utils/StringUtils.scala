package dev.caraml.spark.utils

import com.google.common.hash.Hashing

object StringUtils {
  private def suffixHash(expr: String): String = {
    Hashing.murmur3_32().hashBytes(expr.getBytes).toString
  }

  def trimAndHash(expr: String, maxLength: Int): String = {
    // Length 8 suffix as derived from murmurhash_32 implementation
    val maxPrefixLength = maxLength - 8
    if (expr.length > maxLength)
      expr
        .take(maxPrefixLength)
        .concat(suffixHash(expr.substring(maxPrefixLength)))
    else
      expr
  }
}
