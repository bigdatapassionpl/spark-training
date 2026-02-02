package com.bigdatapassion.spark.streaming.kafka

import com.bigdatapassion.spark.streaming.BaseSparkStreamingApp
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object ProductAvroReader extends BaseSparkStreamingApp {

  // Schema Registry URL
  val schemaRegistryUrl = "http://localhost:8081"

  // Kafka topic with Avro data
  val avroTopic = "test-product-avro"

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession("spark-streaming-kafka-avro")
    import spark.implicits._

    // Fetch schema from Confluent Schema Registry
    val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    val schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(avroTopic + "-value")
    val avroSchema = schemaMetadata.getSchema

    println(s"Using Avro schema: $avroSchema")

    // Read from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", avroTopic)
      .option("startingOffsets", "latest")
      .option("kafka.group.id", kafkaGroupId + "-product-avro")
      .load()

    // Confluent Avro has 5-byte header (1 magic byte + 4 bytes schema ID)
    // Strip the header and deserialize using Spark's native from_avro
    val avroDF = kafkaDF
      .select(
        from_avro(expr("substring(value, 6)"), avroSchema).as("productMessage")
      )

    // Expand the struct into columns (creationDate, id, product)
    val productsDF = avroDF.select(
      $"productMessage.creationDate",
      $"productMessage.id",
      $"productMessage.product.productName",
      $"productMessage.product.color",
      $"productMessage.product.material",
      $"productMessage.product.price",
      $"productMessage.product.promotionCode"
    )

    // Print to console
    val query = productsDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("kafka-avro-products")
      .start()

    query.awaitTermination()
  }

}
