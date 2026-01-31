package com.bigdatapassion.spark.streaming.kafka

import com.bigdatapassion.spark.streaming.openlineage.SchemaRegistryOpenLineageEmitter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import za.co.absa.abris.avro.functions.from_avro
import za.co.absa.abris.config.AbrisConfig

/**
 * Reads Avro messages from Kafka using ABRiS library with OpenLineage Schema Registry integration.
 * Emits custom OpenLineage events with full Avro schema from Schema Registry.
 */
object ProductAvroAbrisReader {

  val master = "local[*]"
  val user = "bigdata"
  val kafkaBootstrapServers = "localhost:9092"
  val kafkaGroupId = "spark-streaming-group"

  // Schema Registry URL
  val schemaRegistryUrl = "http://localhost:8081"

  // Kafka topic with Avro data
  val avroTopic = "test-product-avro"

  // OpenLineage configuration
  val openlineageNamespace = "spark-streaming"
  val openlineageJobName = "product_avro_abris_reader"
  val openlineageFileLocation = "/Users/radek/projects/bigdatapassion/spark-training/openlineage.json"

  def main(args: Array[String]): Unit = {

    // Emit custom OpenLineage event with Schema Registry metadata BEFORE starting Spark
    val runId = SchemaRegistryOpenLineageEmitter.emitSchemaRegistryLineageEvent(
      schemaRegistryUrl = schemaRegistryUrl,
      topic = avroTopic,
      namespace = openlineageNamespace,
      jobName = openlineageJobName,
      openlineageFileLocation = openlineageFileLocation
    )

    // Create Spark session with OpenLineage listener
    val spark = SparkSession.builder()
      .master(master)
      .appName(s"$user ProductAvroAbrisReader")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
      .config("spark.security.credentials.kafka.enabled", "false")
      // OpenLineage transport configuration
      .config("spark.openlineage.transport.type", "composite")
      .config("spark.openlineage.transport.transports.console.type", "console")
      .config("spark.openlineage.transport.transports.file.type", "file")
      .config("spark.openlineage.transport.transports.file.location", openlineageFileLocation)
      .config("spark.openlineage.transport.transports.marquez.type", "http")
      .config("spark.openlineage.transport.transports.marquez.url", "http://localhost:5050")
      .config("spark.openlineage.transport.transports.marquez.endpoint", "api/v1/lineage")
      .config("spark.openlineage.namespace", openlineageNamespace)
      .getOrCreate()

    import spark.implicits._

    // ABRiS configuration for reading Avro with Schema Registry
    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(avroTopic)
      .usingSchemaRegistry(schemaRegistryUrl)

    println(s"ABRiS configured for topic: $avroTopic with Schema Registry: $schemaRegistryUrl")

    // Read from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", avroTopic)
      .option("startingOffsets", "latest")
      .option("kafka.group.id", kafkaGroupId + "-product-avro-abris")
      .load()

    // Deserialize Avro data using ABRiS with Schema Registry integration
    val avroDF = kafkaDF
      .select(from_avro($"value", abrisConfig).as("productMessage"))

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
      .queryName("kafka-avro-products-abris")
      .start()

    // Add shutdown hook to emit COMPLETE event
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      SchemaRegistryOpenLineageEmitter.emitCompleteEvent(
        runId = runId,
        namespace = openlineageNamespace,
        jobName = openlineageJobName,
        openlineageFileLocation = openlineageFileLocation
      )
      println(s"Emitted OpenLineage COMPLETE event for run: $runId")
    }))

    query.awaitTermination()
  }

}
