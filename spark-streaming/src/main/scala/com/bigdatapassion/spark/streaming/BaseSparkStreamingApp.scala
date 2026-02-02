package com.bigdatapassion.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.bigdatapassion.spark.core.BaseSparkApp

trait BaseSparkStreamingApp extends BaseSparkApp {

  /**
   * Creates a SparkSession for Structured Streaming with OpenLineage integration.
   *
   * @param openlineageNamespace the OpenLineage namespace for this job (appears in Marquez UI)
   */
  def createSparkSession(openlineageNamespace: String = "spark-streaming"): SparkSession = {
    SparkSession.builder()
      .master(master)
      .appName(user + " " + this.getClass.getSimpleName)
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
      .config("spark.security.credentials.kafka.enabled", "false")
      // OpenLineage configuration via Spark properties
      .config("spark.openlineage.transport.type", "composite")
      .config("spark.openlineage.transport.transports.console.type", "console")
      .config("spark.openlineage.transport.transports.file.type", "file")
      .config("spark.openlineage.transport.transports.file.location", "/Users/radek/projects/bigdatapassion/spark-training/openlineage.json")
      .config("spark.openlineage.transport.transports.marquez.type", "http")
      .config("spark.openlineage.transport.transports.marquez.url", "http://localhost:5050")
      .config("spark.openlineage.transport.transports.marquez.endpoint", "api/v1/lineage")
      .config("spark.openlineage.namespace", openlineageNamespace)
      .getOrCreate()
  }

  /**
   * Creates a StreamingContext for legacy DStream API.
   * Note: OpenLineage does not support DStream operations.
   */
  def createStreamingContext: StreamingContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(user + " " + this.getClass.getSimpleName)
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
      .set("spark.security.credentials.kafka.enabled", "false")

    new StreamingContext(conf, Seconds(5))
  }

}
