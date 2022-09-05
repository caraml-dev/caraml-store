package dev.caraml.spark.stores.redis

case class SparkRedisConfig(
    namespace: String,
    projectName: String,
    entityColumns: Array[String],
    timestampColumn: String,
    timestampPrefix: String = "_ts",
    repartitionByEntity: Boolean = true,
    maxAge: Long = 0,
    expiryPrefix: String = "_ex"
)

object SparkRedisConfig {
  val NAMESPACE          = "namespace"
  val ENTITY_COLUMNS     = "entity_columns"
  val TS_COLUMN          = "timestamp_column"
  val ENTITY_REPARTITION = "entity_repartition"
  val PROJECT_NAME       = "project_name"
  val MAX_AGE            = "max_age"

  def parse(parameters: Map[String, String]): SparkRedisConfig =
    SparkRedisConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      projectName = parameters.getOrElse(PROJECT_NAME, "default"),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      repartitionByEntity = parameters.getOrElse(ENTITY_REPARTITION, "true") == "true",
      maxAge = parameters.get(MAX_AGE).map(_.toLong).getOrElse(0)
    )
}
