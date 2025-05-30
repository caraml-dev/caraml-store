package dev.caraml.spark

import com.dimafeng.testcontainers.{
  ForAllTestContainer,
  GenericContainer,
  KafkaContainer,
  MultipleContainers
}
import com.example.protos.{AllTypesMessage, InnerMessage, TestMessage, VehicleType}
import com.google.protobuf.{AbstractMessage, ByteString, Timestamp}
import dev.caraml.spark.helpers.DataHelper.{generateDistinctRows, generateTempPath}
import dev.caraml.spark.helpers.RedisStorageHelper.{
  beStoredRow,
  encodeFeatureKey,
  murmurHashHexString
}
import dev.caraml.spark.helpers.TestRow
import dev.caraml.store.protobuf.types.ValueProto.ValueType
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{col, struct}
import org.joda.time.DateTime
import org.scalacheck.Gen
import redis.clients.jedis.Jedis

import java.nio.file.Paths
import java.util.Properties
import scala.collection.JavaConverters._

class StreamingPipelineSpec extends SparkSpec with ForAllTestContainer {
  val redisContainer = GenericContainer("redis:6.0.8", exposedPorts = Seq(6379))
  val kafkaContainer = KafkaContainer()

  override val container = MultipleContainers(redisContainer, kafkaContainer)

  override def withSparkConfOverrides(conf: SparkConf): SparkConf = conf
    .set("spark.redis.host", redisContainer.host)
    .set("spark.redis.port", redisContainer.mappedPort(6379).toString)
    .set("spark.sql.streaming.checkpointLocation", generateTempPath("checkpoint"))
    .set("spark.redis.properties.maxJitter", "0")
    .set("spark.redis.properties.pipelineSize", "250")
    .set("spark.redis.properties.enableRateLimit", "false")
    .set("spark.redis.properties.ratePerSecondLimit", "50000")

  trait KafkaPublisher {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaContainer.bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.VoidSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    val producer = new KafkaProducer[Void, Array[Byte]](props)

    def sendToKafka(topic: String, m: AbstractMessage): Unit = {
      sendToKafka(topic, m.toByteArray)
    }

    def sendToKafka(topic: String, bytes: Array[Byte]): Unit = {
      producer.send(new ProducerRecord[Void, Array[Byte]](topic, null, bytes)).get
    }
  }

  trait Scope extends KafkaPublisher {
    implicit def testRowEncoder: Encoder[TestRow] = ExpressionEncoder()

    val jedis = new Jedis("localhost", redisContainer.mappedPort(6379))
    jedis.flushAll()

    val config = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "driver-fs",
        project = "default",
        entities = Seq(
          Field("s2_id", ValueType.Enum.INT64),
          Field("vehicle_type", ValueType.Enum.STRING)
        ),
        features = Seq(
          Field("unique_drivers", ValueType.Enum.INT64)
        )
      ),
      store =
        RedisConfig("localhost", 6379, properties = RedisWriteProperties(maxJitterSeconds = 0))
    )

    def encodeEntityKey(row: TestMessage, featureTable: FeatureTable): Array[Byte] = {
      val entities = featureTable.entities.map(_.name).mkString("#")
      val key = DigestUtils.md5Hex(
        s"${featureTable.project}#${entities}:${row.getS2Id.toString}#${row.getVehicleType.toString}"
      )
      key.getBytes()
    }

    def groupByEntity(row: TestMessage) =
      new String(encodeEntityKey(row, config.featureTable))

    def rowGenerator =
      for {
        s2_id        <- Gen.chooseNum(0, 1000000)
        vehicle_type <- Gen.oneOf(VehicleType.Enum.BIKE, VehicleType.Enum.CAR)
        drivers      <- Gen.chooseNum(0, 1000)
        eventTimestamp <- Gen
          .choose(0, 300)
          .map(DateTime.now.withMillisOfSecond(0).minusSeconds)
      } yield TestMessage.newBuilder
        .setS2Id(s2_id)
        .setVehicleType(vehicle_type)
        .setUniqueDrivers(drivers)
        .setEventTimestamp(Timestamp.newBuilder.setSeconds(eventTimestamp.getMillis / 1000))
        .build

    val kafkaSource = KafkaSource(
      bootstrapServers = kafkaContainer.bootstrapServers,
      topic = "topic",
      format = ProtoFormat("com.example.protos.TestMessage"),
      fieldMapping = Map.empty,
      eventTimestampColumn = "event_timestamp"
    )
    val featureKeyEncoder: String => String = encodeFeatureKey(config.featureTable)
  }

  "Streaming pipeline" should "store valid proto messages from kafka to redis" in new Scope {
    val configWithKafka = config.copy(source = kafkaSource)
    val query           = StreamingPipeline.createPipeline(sparkSession, configWithKafka).get
    query.processAllAvailable() // to init kafka consumer

    val rows = generateDistinctRows(rowGenerator, 1000, groupByEntity)
    rows.foreach(sendToKafka(kafkaSource.topic, _))

    Thread.sleep(10000); // sleep for 10s to allow topic discovery
    query.processAllAvailable()
    rows.foreach { r =>
      val encodedEntityKey = encodeEntityKey(r, config.featureTable)
      val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("unique_drivers") -> r.getUniqueDrivers,
          murmurHashHexString("_ts:driver-fs") -> new java.sql.Timestamp(
            r.getEventTimestamp.getSeconds * 1000
          )
        )
      )
      val keyTTL = jedis.ttl(encodedEntityKey).toInt
      keyTTL shouldEqual -1
    }

  }

  // "Streaming pipeline" should "store valid proto messages from kafka to redis when reading from topics with prefix" in new Scope {
  //   val configWithKafka = config.copy(source = kafkaSource)
  //   val query           = StreamingPipeline.createPipeline(sparkSession, configWithKafka).get
  //   query.processAllAvailable() // to init kafka consumer

  //   val rowsV2 = generateDistinctRows(rowGenerator, 10, groupByEntity)
  //   rowsV2.foreach(sendToKafka(s"GCP.${kafkaSource.topic}", _))

  //   Thread.sleep(10000); // sleep for 10s to allow topic discovery
  //   query.processAllAvailable()
  //   rowsV2.foreach { r =>
  //     val encodedEntityKey = encodeEntityKey(r, config.featureTable)
  //     val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
  //     print(s"r2: ${r}, storedValues: ${storedValues}")
  //     storedValues should beStoredRow(
  //       Map(
  //         featureKeyEncoder("unique_drivers") -> r.getUniqueDrivers,
  //         murmurHashHexString("_ts:driver-fs") -> new java.sql.Timestamp(
  //           r.getEventTimestamp.getSeconds * 1000
  //         )
  //       )
  //     )
  //     val keyTTL = jedis.ttl(encodedEntityKey).toInt
  //     keyTTL shouldEqual -1
  //   }

  // }
  "Streaming pipeline" should "store messages from kafka to redis with expiry time equal to entity max age" in new Scope {
    val maxAge       = 86400L
    val entityMaxAge = 1728000L
    val configWithMaxAge = config.copy(
      source = kafkaSource,
      featureTable = config.featureTable.copy(maxAge = Some(maxAge)),
      entityMaxAge = Some(entityMaxAge)
    )
    val query = StreamingPipeline.createPipeline(sparkSession, configWithMaxAge).get
    query.processAllAvailable() // to init kafka consumer

    val rows = generateDistinctRows(rowGenerator, 100, groupByEntity)

    rows.foreach(sendToKafka(kafkaSource.topic, _))

    query.processAllAvailable()

    rows.foreach { r =>
      val encodedEntityKey = encodeEntityKey(r, config.featureTable)
      val storedValues     = jedis.hgetAll(encodedEntityKey).asScala.toMap
      storedValues should beStoredRow(
        Map(
          featureKeyEncoder("unique_drivers") -> r.getUniqueDrivers,
          murmurHashHexString("_ts:driver-fs") -> new java.sql.Timestamp(
            r.getEventTimestamp.getSeconds * 1000
          )
        )
      )
      val keyTTL = jedis.ttl(encodedEntityKey).toLong
      keyTTL should (be <= entityMaxAge and be > maxAge)
    }
  }

  "Streaming pipeline" should "store invalid proto messages to deadletter path" in new Scope {
    val configWithDeadletter = config.copy(
      source = kafkaSource,
      deadLetterPath = Some(generateTempPath("deadletters"))
    )
    val query = StreamingPipeline.createPipeline(sparkSession, configWithDeadletter).get
    query.processAllAvailable() // to init kafka consumer

    val rows = generateDistinctRows(rowGenerator, 1000, groupByEntity).map(
      _.toBuilder.clearVehicleType().build()
    )

    rows.foreach(sendToKafka(kafkaSource.topic, _))
    query.processAllAvailable()

    // ingest twice to check that rows are appended to deadletter path
    rows.foreach(sendToKafka(kafkaSource.topic, _))
    query.processAllAvailable()

    sparkSession.read
      .parquet(
        Paths
          .get(configWithDeadletter.deadLetterPath.get, sparkSession.conf.get("spark.app.id"))
          .toString
      )
      .count() should be(2 * rows.length)
  }

  "All protobuf types" should "be correctly converted" in new Scope {
    val configWithKafka = config.copy(
      source = kafkaSource.copy(
        format = ProtoFormat("com.example.protos.AllTypesMessage"),
        fieldMapping = Map(
          "map_value"     -> "map.key",
          "inner_double"  -> "inner.double",
          "inner_float"   -> "inner.float",
          "inner_integer" -> "inner.integer",
          "inner_long"    -> "inner.long"
        )
      ),
      featureTable = FeatureTable(
        name = "all-types-fs",
        project = "default",
        entities = Seq(
          Field("string", ValueType.Enum.STRING)
        ),
        features = Seq(
          Field("double", ValueType.Enum.DOUBLE),
          Field("float", ValueType.Enum.FLOAT),
          Field("integer", ValueType.Enum.INT32),
          Field("long", ValueType.Enum.INT64),
          Field("uinteger", ValueType.Enum.INT32),
          Field("ulong", ValueType.Enum.INT64),
          Field("sinteger", ValueType.Enum.INT32),
          Field("slong", ValueType.Enum.INT64),
          Field("finteger", ValueType.Enum.INT32),
          Field("flong", ValueType.Enum.INT64),
          Field("sfinteger", ValueType.Enum.INT32),
          Field("sflong", ValueType.Enum.INT64),
          Field("bool", ValueType.Enum.BOOL),
          Field("bytes", ValueType.Enum.BYTES),
          Field("map_value", ValueType.Enum.STRING),
          Field("inner_double", ValueType.Enum.DOUBLE_LIST),
          Field("inner_float", ValueType.Enum.FLOAT_LIST),
          Field("inner_integer", ValueType.Enum.INT32_LIST),
          Field("inner_long", ValueType.Enum.INT64_LIST)
        )
      )
    )
    val query = StreamingPipeline.createPipeline(sparkSession, configWithKafka).get
    query.processAllAvailable() // to init kafka consumer

    val message = AllTypesMessage.newBuilder
      .setDouble(1.0)
      .setFloat(1.0.toFloat)
      .setLong(1L)
      .setInteger(1)
      .setUinteger(1)
      .setUlong(1L)
      .setSlong(1L)
      .setSinteger(1)
      .setFlong(1L)
      .setFinteger(1)
      .setSfinteger(1)
      .setSflong(1L)
      .setBool(true)
      .setString("test")
      .setBytes(ByteString.copyFrom("test", "UTF-8"))
      .putAllMap(Map("key" -> "value", "key2" -> "value2").asJava)
      .setEventTimestamp(Timestamp.newBuilder().setSeconds(DateTime.now.getMillis / 1000))
      .setInner(
        InnerMessage
          .newBuilder()
          .addDouble(1)
          .addFloat(1)
          .addInteger(1)
          .addLong(1)
          .setEnum(InnerMessage.Enum.one)
      )
      .build

    sendToKafka(kafkaSource.topic, message)
    query.processAllAvailable()

    val allTypesKeyEncoder: String => String = encodeFeatureKey(configWithKafka.featureTable)
    val redisKey                             = DigestUtils.md5Hex(s"default#string:test").getBytes()
    val storedValues                         = jedis.hgetAll(redisKey).asScala.toMap
    storedValues should beStoredRow(
      Map(
        allTypesKeyEncoder("double")        -> 1,
        allTypesKeyEncoder("float")         -> 1.0,
        allTypesKeyEncoder("long")          -> 1,
        allTypesKeyEncoder("integer")       -> 1,
        allTypesKeyEncoder("slong")         -> 1,
        allTypesKeyEncoder("sinteger")      -> 1,
        allTypesKeyEncoder("flong")         -> 1,
        allTypesKeyEncoder("finteger")      -> 1,
        allTypesKeyEncoder("sflong")        -> 1,
        allTypesKeyEncoder("sfinteger")     -> 1,
        allTypesKeyEncoder("bool")          -> true,
        allTypesKeyEncoder("bytes")         -> ByteString.copyFrom("test", "UTF-8"),
        allTypesKeyEncoder("map_value")     -> "value",
        allTypesKeyEncoder("inner_double")  -> List(1.0),
        allTypesKeyEncoder("inner_float")   -> List(1.0f),
        allTypesKeyEncoder("inner_integer") -> List(1),
        allTypesKeyEncoder("inner_long")    -> List(1L)
      )
    )
  }

  "Expected feature types" should "match source types" in new Scope {
    val configWithKafka = config.copy(
      source = kafkaSource,
      featureTable = FeatureTable(
        name = "driver-fs",
        project = "default",
        entities = Seq(
          Field("s2_id", ValueType.Enum.STRING),
          Field("vehicle_type", ValueType.Enum.INT32)
        ),
        features = Seq(
          Field("unique_drivers", ValueType.Enum.FLOAT)
        )
      )
    )

    assertThrows[RuntimeException] {
      StreamingPipeline.createPipeline(sparkSession, configWithKafka).get
    }
  }

  "Streaming pipeline" should "store valid avro messages from kafka to redis" in new Scope {
    val avroConfig = IngestionJobConfig(
      featureTable = FeatureTable(
        name = "test-fs",
        project = "default",
        entities = Seq(Field("customer", ValueType.Enum.STRING)),
        features = Seq(
          Field("feature1", ValueType.Enum.INT32),
          Field("feature2", ValueType.Enum.FLOAT)
        )
      ),
      source = KafkaSource(
        bootstrapServers = kafkaContainer.bootstrapServers,
        topic = "avro",
        format = AvroFormat(schemaJson = """{
            |"type": "record",
            |"name": "TestMessage",
            |"fields": [
            |{"name": "customer", "type": ["string","null"]},
            |{"name": "feature1", "type": "int"},
            |{"name": "feature2", "type": "float"},
            |{"name": "eventTimestamp", "type": [{"type": "long", "logicalType": "timestamp-micros"}, "null"]}
            |]
            |}""".stripMargin),
        fieldMapping = Map.empty,
        eventTimestampColumn = "eventTimestamp"
      )
    )
    val query = StreamingPipeline.createPipeline(sparkSession, avroConfig).get
    query.processAllAvailable() // to init kafka consumer

    val row = TestRow("aaa", 1, 0.5f, new java.sql.Timestamp(DateTime.now.withMillis(0).getMillis))
    val df  = sparkSession.createDataFrame(Seq(row))
    df
      .select(
        to_avro(
          struct(
            col("customer"),
            col("feature1"),
            col("feature2"),
            col("eventTimestamp")
          )
        ).alias("value")
      )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaContainer.bootstrapServers)
      .option("topic", "avro")
      .save()
    Thread.sleep(10000)
    query.processAllAvailable()

    val redisKey = DigestUtils.md5Hex(s"default#customer:aaa").getBytes()

    val storedValues                              = jedis.hgetAll(redisKey).asScala.toMap
    val customFeatureKeyEncoder: String => String = encodeFeatureKey(avroConfig.featureTable)
    storedValues should beStoredRow(
      Map(
        customFeatureKeyEncoder("feature1") -> row.feature1,
        customFeatureKeyEncoder("feature2") -> row.feature2,
        murmurHashHexString("_ts:test-fs")  -> row.eventTimestamp
      )
    )

  }

  "Streaming pipeline" should "correctly handle invalid message from stream" in new Scope {
    val configWithDeadletter = config.copy(
      source = kafkaSource,
      deadLetterPath = Some(generateTempPath("deadletters"))
    )
    val query = StreamingPipeline.createPipeline(sparkSession, configWithDeadletter).get
    query.processAllAvailable() // to init kafka consumer

    sendToKafka(kafkaSource.topic, "invalid".getBytes)

    query.processAllAvailable()

    jedis.keys("*").asScala.toList.length should be(0)
  }
}
