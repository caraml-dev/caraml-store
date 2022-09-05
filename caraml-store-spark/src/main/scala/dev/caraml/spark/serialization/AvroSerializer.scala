package dev.caraml.spark.serialization

import com.google.common.hash.Hashing
import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.types.StructType

class AvroSerializer extends Serializer {
  override type SchemaType = String

  def convertSchema(schema: StructType): String = {
    val avroSchema = SchemaConverters.toAvroType(schema)
    avroSchema.toString
  }

  def schemaReference(schema: String): Array[Byte] = {
    Hashing.murmur3_32().hashBytes(schema.getBytes).asBytes()
  }

  def serializeData(schema: String): Column => Column = to_avro(_, schema)
}
