package com.bigdatapassion.spark.streaming.kafka

import com.bigdatapassion.spark.core.BaseSparkApp
import com.bigdatapassion.spark.streaming.BaseSparkStreamingApp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object WordCount extends BaseSparkStreamingApp with BaseSparkApp {

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession("spark-streaming-kafka-simple")
    import spark.implicits._

    // Read from Kafka using Structured Streaming
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopics)
      .option("startingOffsets", "latest")
      .option("kafka.group.id", kafkaGroupId)
      .load()

    // Extract the value as string
    val messages = kafkaDF
      .selectExpr("CAST(value AS STRING) as message")

    // Print raw messages to console
    val messagesQuery = messages.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("kafka-messages")
      .start()

    // Word count - split messages into words and count
    val words = messages
      .select(explode(split($"message", " ")).as("word"))
      .filter($"word" =!= "")

    val wordCounts = words
      .groupBy($"word")
      .count()

    // Output word counts to console
    val wordCountQuery = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("kafka-word-count")
      .start()

    // Wait for termination
    spark.streams.awaitAnyTermination()
  }

}
