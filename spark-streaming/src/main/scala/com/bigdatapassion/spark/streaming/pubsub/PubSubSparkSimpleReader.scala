package com.bigdatapassion.spark.streaming.pubsub

import com.bigdatapassion.spark.core.BaseSparkApp
import com.bigdatapassion.spark.streaming.BaseSparkStreamingApp
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters._

/**
 * Word count example using Google Pub/Sub as the source.
 *
 * Prerequisites:
 * 1. Create a Pub/Sub topic and subscription in GCP
 * 2. Run: gcloud auth application-default login
 * 3. Update the configuration values below
 *
 * To create topic and subscription:
 * gcloud pubsub topics create spark-word-count-topic
 * gcloud pubsub subscriptions create spark-streaming-subscription --topic=spark-word-count-topic
 *
 * To publish test messages:
 * gcloud pubsub topics publish spark-word-count-topic --message="hello world test message"
 */
object PubSubSparkSimpleReader extends BaseSparkStreamingApp with BaseSparkApp {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val subscriptionId = "spark-streaming-subscription"

  // Processing interval in milliseconds
  val processingIntervalMs = 5000L

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession
    import spark.implicits._

    val subscriptionName = ProjectSubscriptionName.of(gcpProject, subscriptionId)
    println(s"Reading from Pub/Sub subscription: $subscriptionName")

    // Message buffer for collecting messages from Pub/Sub
    val messageBuffer = new ConcurrentLinkedQueue[(String, String)]()

    // Create message receiver
    val receiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        val data = message.getData.toStringUtf8
        val messageId = message.getMessageId
        messageBuffer.offer((messageId, data))
        consumer.ack()
      }
    }

    // Start subscriber in background
    val subscriber = Subscriber.newBuilder(subscriptionName, receiver).build()
    subscriber.startAsync().awaitRunning()
    println("Pub/Sub subscriber started")

    // Word counts accumulator (for complete output mode simulation)
    var wordCountsMap = scala.collection.mutable.Map[String, Long]()

    try {
      while (true) {
        // Collect messages from buffer
        val messages = scala.collection.mutable.ArrayBuffer[(String, String)]()
        var msg = messageBuffer.poll()
        while (msg != null) {
          messages += msg
          msg = messageBuffer.poll()
        }

        if (messages.nonEmpty) {
          println(s"\n--- Processing ${messages.size} messages ---")

          // Create DataFrame from messages
          val schema = StructType(Seq(
            StructField("message_id", StringType, nullable = false),
            StructField("message", StringType, nullable = false)
          ))

          val rows = messages.map { case (id, data) => Row(id, data) }
          val messagesDF = spark.createDataFrame(rows.asJava, schema)

          // Print raw messages
          println("Raw messages:")
          messagesDF.show(truncate = false)

          // Word count
          val words = messagesDF
            .select(explode(split($"message", " ")).as("word"))
            .filter($"word" =!= "")

          val batchWordCounts = words
            .groupBy($"word")
            .count()
            .collect()

          // Update global word counts
          batchWordCounts.foreach { row =>
            val word = row.getString(0)
            val count = row.getLong(1)
            wordCountsMap(word) = wordCountsMap.getOrElse(word, 0L) + count
          }

          // Print updated word counts
          println("\nWord counts (cumulative):")
          val wordCountsDF = spark.createDataFrame(
            wordCountsMap.toSeq.map { case (w, c) => (w, c) }
          ).toDF("word", "count")
            .orderBy($"count".desc)

          wordCountsDF.show(truncate = false)
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
