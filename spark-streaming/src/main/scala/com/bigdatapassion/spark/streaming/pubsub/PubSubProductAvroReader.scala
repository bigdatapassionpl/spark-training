package com.bigdatapassion.spark.streaming.pubsub

import com.bigdatapassion.spark.core.BaseSparkApp
import com.bigdatapassion.spark.streaming.BaseSparkStreamingApp
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, SchemaServiceClient, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, SchemaName}
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters._

/**
 * Spark Streaming application that reads Avro-encoded product messages from Pub/Sub
 * using GCP Schema Registry.
 *
 * Prerequisites:
 * 1. Create a schema in GCP Schema Registry
 * 2. Create a Pub/Sub topic with the schema
 * 3. Run: gcloud auth application-default login
 *
 * Setup commands:
 * gcloud pubsub schemas create product-avro-schema --type=avro --definition-file=product-schema.avsc
 * gcloud pubsub topics create product-avro-topic --schema=product-avro-schema --message-encoding=binary
 * gcloud pubsub subscriptions create product-avro-subscription --topic=product-avro-topic
 */
object PubSubProductAvroReader extends BaseSparkStreamingApp with BaseSparkApp {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val subscriptionId = "product-avro-subscription"
  val schemaId = "product-avro-schema"

  // Processing interval in milliseconds
  val processingIntervalMs = 5000L

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
    import spark.implicits._

    // Fetch schema from GCP Schema Registry
    val schemaName = SchemaName.of(gcpProject, schemaId)
    println(s"Fetching schema from GCP Schema Registry: $schemaName")

    val schemaServiceClient = SchemaServiceClient.create()
    val gcpSchema = schemaServiceClient.getSchema(schemaName)
    val avroSchemaJson = gcpSchema.getDefinition
    schemaServiceClient.close()

    println(s"Schema fetched successfully:\n$avroSchemaJson")

    val subscriptionName = ProjectSubscriptionName.of(gcpProject, subscriptionId)
    println(s"Reading Avro messages from Pub/Sub subscription: $subscriptionName")

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

          // Deserialize Avro data using Spark's native from_avro with schema from registry
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
