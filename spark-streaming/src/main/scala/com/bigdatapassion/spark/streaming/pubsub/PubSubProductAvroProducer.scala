package com.bigdatapassion.spark.streaming.pubsub

import com.github.javafaker.Faker
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.avro.generic.GenericDatumWriter

import java.io.ByteArrayOutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

/**
 * Pub/Sub producer that sends Avro-encoded product messages.
 *
 * Prerequisites:
 * 1. Create a Pub/Sub topic in GCP
 * 2. Run: gcloud auth application-default login
 * 3. Update the configuration values below
 *
 * To create topic and subscription:
 * gcloud pubsub topics create product-avro-topic
 * gcloud pubsub subscriptions create product-avro-subscription --topic=product-avro-topic
 */
object PubSubProductAvroProducer {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val topicId = "product-avro-topic"

  // Producer settings
  val messageBatchCount = 5
  val sleepMs = 2000L

  // Avro schema for ProductMessage
  val avroSchemaJson: String =
    """
      |{
      |  "type": "record",
      |  "name": "ProductMessageAvro",
      |  "namespace": "com.bigdatapassion.pubsub.dto",
      |  "fields": [
      |    {"name": "creationDate", "type": ["null", "string"]},
      |    {"name": "id", "type": ["null", "long"]},
      |    {
      |      "name": "product",
      |      "type": ["null", {
      |        "type": "record",
      |        "name": "ProductAvro",
      |        "fields": [
      |          {"name": "productName", "type": ["null", "string"]},
      |          {"name": "color", "type": ["null", "string"]},
      |          {"name": "material", "type": ["null", "string"]},
      |          {"name": "price", "type": ["null", "string"]},
      |          {"name": "promotionCode", "type": ["null", "string"]}
      |        ]
      |      }]
      |    }
      |  ]
      |}
      |""".stripMargin

  private val schema = new Schema.Parser().parse(avroSchemaJson)
  private val productSchema = schema.getField("product").schema().getTypes.get(1)
  private val faker = new Faker()
  private val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val topicName = TopicName.of(gcpProject, topicId)
    println(s"Publishing Avro messages to Pub/Sub topic: $topicName")
    println(s"Using schema:\n$avroSchemaJson")

    val publisher = Publisher.newBuilder(topicName).build()

    var messageId = 0L

    try {
      while (true) {
        val futures = (1 to messageBatchCount).map { _ =>
          messageId += 1

          val productMessage = createProductMessage(messageId)
          val avroBytes = serializeToAvro(productMessage)

          val message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(avroBytes))
            .putAttributes("messageId", messageId.toString)
            .putAttributes("contentType", "application/avro")
            .build()

          println(s"[$messageId] Sending product: ${productMessage.get("product").asInstanceOf[GenericRecord].get("productName")}")

          publisher.publish(message)
        }

        // Wait for all messages in batch to be published
        futures.foreach { future =>
          val publishedId = future.get()
          println(s"Published with ID: $publishedId")
        }

        Thread.sleep(sleepMs)
      }
    } catch {
      case e: Exception =>
        System.err.println(s"Error publishing messages: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      publisher.shutdown()
      publisher.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  private def createProductMessage(id: Long): GenericRecord = {
    val product = new GenericData.Record(productSchema)
    product.put("productName", faker.commerce().productName())
    product.put("color", faker.commerce().color())
    product.put("material", faker.commerce().material())
    product.put("price", faker.commerce().price())
    product.put("promotionCode", faker.commerce().promotionCode())

    val message = new GenericData.Record(schema)
    message.put("creationDate", LocalDateTime.now().format(formatter))
    message.put("id", id)
    message.put("product", product)

    message
  }

  private def serializeToAvro(record: GenericRecord): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val writer: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(outputStream, null)

    writer.write(record, encoder)
    encoder.flush()
    outputStream.close()

    outputStream.toByteArray
  }
}
