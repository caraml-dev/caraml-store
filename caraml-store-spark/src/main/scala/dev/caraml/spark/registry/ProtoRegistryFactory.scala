package dev.caraml.spark.registry

import org.apache.spark.sql.SparkSession

object ProtoRegistryFactory {
  val CONFIG_PREFIX       = "feast.ingestion.registry.proto."
  val PROTO_REGISTRY_KIND = s"${CONFIG_PREFIX}kind"
  val DEFAULT_KIND        = "local"

  def resolveProtoRegistry(sparkSession: SparkSession): ProtoRegistry = {
    val config     = sparkSession.sparkContext.getConf
    val kind       = config.get(PROTO_REGISTRY_KIND, DEFAULT_KIND)
    val properties = config.getAllWithPrefix(CONFIG_PREFIX).toMap
    protoRegistry(kind, properties)
  }

  private def protoRegistry(name: String, properties: Map[String, String]): ProtoRegistry =
    name match {
      case "local"   => new LocalProtoRegistry
      case "stencil" => new StencilProtoRegistry(properties("url"), properties.get("token"))
    }
}
