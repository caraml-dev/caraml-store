package dev.caraml.spark.results
import dev.caraml.spark.IngestionJobConfig
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats}
import org.joda.time.DateTime

class KafkaResultStore(bootstrapServer: String, topic: String) extends BaseResultStore {
  val props = new Properties()
  props.put("bootstrap.servers", bootstrapServer)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  implicit val formats: Formats = DefaultFormats

  private val producer = new KafkaProducer[String, String](props)

  override def storeResults(config: IngestionJobConfig, numRows: Long): Unit = {
    val data = KafkaResultData(
      config.featureTable.name,
      numRows,
      config.startTime,
      config.endTime,
      config.source.toString,
      config.store.toString
    )
    val jsonString = write(data) // Convert config to JSON string
    val record = new ProducerRecord[String, String](topic, jsonString)
    producer.send(record)
    println(s"Storing results in Kafka with config $config and numRows $numRows")
  }

  def close(): Unit = {
    producer.close()
  }

case class KafkaResultData(
    featureTable: String,
    numRows: Long,
    startTime: DateTime,
    endTime: DateTime,
    dataSource: String,
    onlineStore: String
)
}