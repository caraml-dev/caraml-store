package dev.caraml.spark.registry

import com.example.protos.TestMessage
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import dev.caraml.spark.SparkSpec
import dev.caraml.spark.helpers.DataHelper.generateTempPath
import dev.caraml.spark.utils.ProtoReflection
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

case class BinaryRecord(bytes: Array[Byte])

class StencilSpec extends SparkSpec with BeforeAndAfterAll {
  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.sql.streaming.checkpointLocation", generateTempPath("checkpoint"))
    .set("feast.ingestion.registry.proto.kind", "stencil")
    .set(
      "feast.ingestion.registry.proto.url",
      s"http://localhost:${wireMockConfig.portNumber()}/descriptor.desc"
    )

  val wireMockConfig = (new WireMockConfiguration)
    .withRootDirectory(getClass.getResource("/stencil").getPath)
    .port(8082)
  val wireMockServer = new WireMockServer(wireMockConfig)

  override def beforeAll(): Unit =
    wireMockServer.start()

  override def afterAll(): Unit =
    wireMockServer.stop()

  trait Scope {
    implicit val encoder: ExpressionEncoder[BinaryRecord] = ExpressionEncoder[BinaryRecord]
  }

  "Proto parser" should "be able to retrieve proto descriptors from external repos" in new Scope {
    val testMessage = TestMessage
      .newBuilder()
      .setS2Id(1)
      .setUniqueDrivers(100)
      .build()

    val protoRegistry = ProtoRegistryFactory.resolveProtoRegistry(sparkSession)
    val className     = "com.example.protos.TestMessage"

    val parser: Array[Byte] => Row = ProtoReflection.createMessageParser(protoRegistry, className)
    val parseUDF =
      udf(parser, ProtoReflection.inferSchema(protoRegistry.getProtoDescriptor(className)))

    val parsed = sparkSession
      .createDataset(Seq(BinaryRecord(testMessage.toByteArray)))
      .withColumn("parsed", parseUDF(col("bytes")))
      .select("parsed.*")

    parsed.schema should be(
      StructType(
        StructField("s2_id", LongType, true) ::
          StructField("vehicle_type", StringType, true) ::
          StructField("unique_drivers", LongType, true) ::
          StructField("event_timestamp", TimestampType, true) :: Nil
      )
    )

    parsed.collect() should contain(
      Row(
        1L,
        null,
        100,
        null
      )
    )
  }
}
