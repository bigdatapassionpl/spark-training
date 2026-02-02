package com.bigdatapassion.spark.streaming.pubsub

import com.github.javafaker.Faker
import com.google.cloud.pubsub.v1.{Publisher, SchemaServiceClient}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, SchemaName, TopicName}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.generic.GenericDatumWriter

import java.io.ByteArrayOutputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

/**
 * Pub/Sub producer that sends Avro-encoded product messages using GCP Schema Registry.
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
object PubSubProductAvroProducer {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val topicId = "product-avro-topic"
  val schemaId = "product-avro-schema"

  // Producer settings
  val messageBatchCount = 5
  val sleepMs = 2000L

  private val faker = new Faker()
  private val formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss")

  def main(args: Array[String]): Unit = {

    // Fetch schema from GCP Schema Registry
    val schemaName = SchemaName.of(gcpProject, schemaId)
    println(s"Fetching schema from GCP Schema Registry: $schemaName")

    val schemaServiceClient = SchemaServiceClient.create()
    val gcpSchema = schemaServiceClient.getSchema(schemaName)
    val avroSchemaJson = gcpSchema.getDefinition
    schemaServiceClient.close()

    println(s"Schema fetched successfully:\n$avroSchemaJson")

    // Parse Avro schema
    val schema = new Schema.Parser().parse(avroSchemaJson)
    val productSchema = schema.getField("product").schema().getTypes.get(1)

    val topicName = TopicName.of(gcpProject, topicId)
    println(s"Publishing Avro messages to Pub/Sub topic: $topicName")

    val publisher = Publisher.newBuilder(topicName).build()

    var messageId = 0L

    try {
      while (true) {
        val futures = (1 to messageBatchCount).map { _ =>
          messageId += 1

          val productMessage = createProductMessage(messageId, schema, productSchema)
          val avroBytes = serializeToAvro(productMessage, schema)

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

  private def createProductMessage(id: Long, schema: Schema, productSchema: Schema): GenericRecord = {
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

  private def serializeToAvro(record: GenericRecord, schema: Schema): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(outputStream, null)

    writer.write(record, encoder)
    encoder.flush()
    outputStream.close()

    outputStream.toByteArray
  }
}
