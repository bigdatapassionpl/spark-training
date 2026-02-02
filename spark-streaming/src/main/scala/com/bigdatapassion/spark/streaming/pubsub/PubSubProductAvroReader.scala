package com.bigdatapassion.spark.streaming.pubsub

import com.bigdatapassion.spark.core.BaseSparkApp
import com.bigdatapassion.spark.streaming.BaseSparkStreamingApp
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters._

/**
 * Spark Streaming application that reads Avro-encoded product messages from Pub/Sub.
 *
 * Prerequisites:
 * 1. Create a Pub/Sub topic and subscription in GCP
 * 2. Run: gcloud auth application-default login
 * 3. Update the configuration values below
 *
 * To create topic and subscription:
 * gcloud pubsub topics create product-avro-topic
 * gcloud pubsub subscriptions create product-avro-subscription --topic=product-avro-topic
 */
object PubSubProductAvroReader extends BaseSparkStreamingApp with BaseSparkApp {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val subscriptionId = "product-avro-subscription"

  // Processing interval in milliseconds
  val processingIntervalMs = 5000L

  // Avro schema (must match the producer schema)
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

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
    import spark.implicits._

    val subscriptionName = ProjectSubscriptionName.of(gcpProject, subscriptionId)
    println(s"Reading Avro messages from Pub/Sub subscription: $subscriptionName")
    println(s"Using schema:\n$avroSchemaJson")

    // Message buffer for collecting messages from Pub/Sub
    val messageBuffer = new ConcurrentLinkedQueue[(String, Array[Byte])]()

    // Create message receiver
    val receiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        val data = message.getData.toByteArray
        val messageId = message.getMessageId
        messageBuffer.offer((messageId, data))
        consumer.ack()
      }
    }

    // Start subscriber in background
    val subscriber = Subscriber.newBuilder(subscriptionName, receiver).build()
    subscriber.startAsync().awaitRunning()
    println("Pub/Sub subscriber started")

    try {
      while (true) {
        // Collect messages from buffer
        val messages = scala.collection.mutable.ArrayBuffer[(String, Array[Byte])]()
        var msg = messageBuffer.poll()
        while (msg != null) {
          messages += msg
          msg = messageBuffer.poll()
        }

        if (messages.nonEmpty) {
          println(s"\n--- Processing ${messages.size} Avro messages ---")

          // Create DataFrame with binary data
          val schema = StructType(Seq(
            StructField("message_id", StringType, nullable = false),
            StructField("avro_data", BinaryType, nullable = false)
          ))

          val rows = messages.map { case (id, data) => Row(id, data) }
          val rawDF = spark.createDataFrame(rows.asJava, schema)

          // Deserialize Avro data using Spark's native from_avro
          val avroDF = rawDF.select(
            $"message_id",
            from_avro($"avro_data", avroSchemaJson).as("productMessage")
          )

          // Expand the struct into columns
          val productsDF = avroDF.select(
            $"message_id",
            $"productMessage.creationDate".as("creationDate"),
            $"productMessage.id".as("id"),
            $"productMessage.product.productName".as("productName"),
            $"productMessage.product.color".as("color"),
            $"productMessage.product.material".as("material"),
            $"productMessage.product.price".as("price"),
            $"productMessage.product.promotionCode".as("promotionCode")
          )

          // Print products
          println("Products received:")
          productsDF.show(truncate = false)
        }

        Thread.sleep(processingIntervalMs)
      }
    } catch {
      case e: InterruptedException =>
        println("Processing interrupted")
    } finally {
      subscriber.stopAsync().awaitTerminated(30, TimeUnit.SECONDS)
      spark.stop()
    }
  }
}
