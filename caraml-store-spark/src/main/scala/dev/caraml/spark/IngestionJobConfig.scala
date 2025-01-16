package dev.caraml.spark

import dev.caraml.spark.Modes.Modes
import dev.caraml.spark.validation.Expectation
import dev.caraml.store.protobuf.types.ValueProto.ValueType
import org.joda.time.DateTime

object Modes extends Enumeration {
  type Modes = Value
  val Offline, Online = Value
}

abstract class StoreConfig

case class RedisConfig(
    host: String,
    port: Int,
    password: String = "",
    ssl: Boolean = false,
    properties: RedisWriteProperties = RedisWriteProperties()
) extends StoreConfig
case class RedisWriteProperties(
    maxJitterSeconds: Int = 3600,
    pipelineSize: Int = 250,
    ttlSeconds: Long = 0L,
    enableRateLimit: Boolean = false,
    ratePerSecondLimit: Int = 50000
)
case class BigTableConfig(projectId: String, instanceId: String) extends StoreConfig
case class HBaseConfig(
    zookeeperQuorum: String,
    zookeeperPort: Int,
    hbaseProperties: HBaseProperties = HBaseProperties()
) extends StoreConfig
case class HBaseProperties(
    regionSplitPolicy: String =
      "org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy",
    compressionAlgorithm: String = "ZSTD"
)

sealed trait MetricConfig

case class StatsDConfig(host: String, port: Int) extends MetricConfig

abstract class DataFormat
case object ParquetFormat                 extends DataFormat
case class ProtoFormat(classPath: String) extends DataFormat
case class AvroFormat(schemaJson: String) extends DataFormat

abstract class Source {
  def fieldMapping: Map[String, String]

  def eventTimestampColumn: String
  def createdTimestampColumn: Option[String]
  def datePartitionColumn: Option[String]
}

abstract class BatchSource extends Source

abstract class StreamingSource extends Source {
  def format: DataFormat
}

case class FileSource(
    path: String,
    override val fieldMapping: Map[String, String],
    override val eventTimestampColumn: String,
    override val createdTimestampColumn: Option[String] = None,
    override val datePartitionColumn: Option[String] = None
) extends BatchSource

case class BQConfig(materialization: Option[BQMaterializationConfig])

case class BQMaterializationConfig(project: String, dataset: String)

case class BQSource(
    project: String,
    dataset: String,
    table: String,
    override val fieldMapping: Map[String, String],
    override val eventTimestampColumn: String,
    override val createdTimestampColumn: Option[String] = None,
    override val datePartitionColumn: Option[String] = None
) extends BatchSource

case class KafkaSource(
    bootstrapServers: String,
    topic: String,
    override val format: DataFormat,
    override val fieldMapping: Map[String, String],
    override val eventTimestampColumn: String,
    override val createdTimestampColumn: Option[String] = None,
    override val datePartitionColumn: Option[String] = None
) extends StreamingSource

case class Sources(
    file: Option[FileSource] = None,
    bq: Option[BQSource] = None,
    kafka: Option[KafkaSource] = None
)

case class Field(name: String, `type`: ValueType.Enum)

case class FeatureTable(
    name: String,
    project: String,
    entities: Seq[Field],
    features: Seq[Field],
    maxAge: Option[Long] = None,
    labels: Map[String, String] = Map.empty
)

case class ValidationConfig(
    name: String,
    pickledCodePath: String,
    includeArchivePath: String
)

case class ExpectationSpec(
    expectations: List[Expectation]
)

abstract class ResultStoreConfig

case class KafkaResultStoreConfig(
    bootstrapServers: String,
    topic: String
) extends ResultStoreConfig


case class IngestionJobConfig(
    mode: Modes = Modes.Offline,
    featureTable: FeatureTable = null,
    entityMaxAge: Option[Long] = None,
    source: Source = null,
    startTime: DateTime = DateTime.now(),
    endTime: DateTime = DateTime.now(),
    store: StoreConfig = RedisConfig("localhost", 6379, "", false),
    metrics: Option[MetricConfig] = None,
    deadLetterPath: Option[String] = None,
    stencilURL: Option[String] = None,
    stencilToken: Option[String] = None,
    streamingTriggeringSecs: Int = 0,
    validationConfig: Option[ValidationConfig] = None,
    expectationSpec: Option[ExpectationSpec] = None,
    doNotIngestInvalidRows: Boolean = false,
    checkpointPath: Option[String] = None,
    bq: Option[BQConfig] = None,
    resultStore: Option[ResultStoreConfig] = None
)
