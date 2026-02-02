package com.bigdatapassion.spark.streaming.pubsub

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}

import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * Simple Pub/Sub producer that sends fake messages for word count testing.
 *
 * Prerequisites:
 * 1. Create a Pub/Sub topic in GCP
 * 2. Run: gcloud auth application-default login
 * 3. Update the configuration values below
 *
 * To create topic and subscription:
 * gcloud pubsub topics create spark-word-count-topic
 * gcloud pubsub subscriptions create spark-streaming-subscription --topic=spark-word-count-topic
 */
object PubSubSimpleProducer {

  // Pub/Sub configuration
  val gcpProject = "bigdataworkshops"
  val topicId = "spark-word-count-topic"

  // Producer settings
  val messageBatchCount = 10
  val sleepMs = 1000L

  // Sample messages for word count
  val messages: Array[String] = Array(
    "Ala ma kota Ela ma psa",
    "W Szczebrzeszynie chrzaszcz brzmi w trzcinie",
    "Byc albo nie byc",
    "Hello world from Pub Sub",
    "Spark Structured Streaming is awesome",
    "Big data processing with Apache Spark",
    "Real time analytics with streaming data",
    "Cloud native applications are the future"
  )

  private val random = new Random(System.currentTimeMillis())

  def main(args: Array[String]): Unit = {

    val topicName = TopicName.of(gcpProject, topicId)
    println(s"Publishing to Pub/Sub topic: $topicName")

    val publisher = Publisher.newBuilder(topicName).build()

    var messageId = 0L

    try {
      while (true) {
        val futures = (1 to messageBatchCount).map { _ =>
          messageId += 1
          val messageText = messages(random.nextInt(messages.length))

          val message = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(messageText))
            .putAttributes("messageId", messageId.toString)
            .build()

          println(s"[$messageId] Sending message: $messageText")

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
}
