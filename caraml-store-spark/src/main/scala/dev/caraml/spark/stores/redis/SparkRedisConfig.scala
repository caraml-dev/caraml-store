package dev.caraml.spark.stores.redis

case class SparkRedisConfig(
    namespace: String,
    projectName: String,
    entityColumns: Array[String],
    timestampColumn: String,
    entityMaxAge: Long = 0L,
    timestampPrefix: String = "_ts"
)

object SparkRedisConfig {
  val NAMESPACE      = "namespace"
  val ENTITY_COLUMNS = "entity_columns"
  val TS_COLUMN      = "timestamp_column"
  val PROJECT_NAME   = "project_name"
  val ENTITY_MAX_AGE = "entity_max_age"

  def parse(parameters: Map[String, String]): SparkRedisConfig =
    SparkRedisConfig(
      namespace = parameters.getOrElse(NAMESPACE, ""),
      projectName = parameters.getOrElse(PROJECT_NAME, "default"),
      entityColumns = parameters.getOrElse(ENTITY_COLUMNS, "").split(","),
      timestampColumn = parameters.getOrElse(TS_COLUMN, "event_timestamp"),
      entityMaxAge = parameters.get(ENTITY_MAX_AGE).map(_.toLong).getOrElse(0)
    )
}
