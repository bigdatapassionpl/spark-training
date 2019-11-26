package com.bigdatapassion.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.bigdatapassion.spark.core.BaseSparkApp

trait BaseSparkStreamingApp extends BaseSparkApp {

  def createStreamingContext: StreamingContext = {

    val conf = new SparkConf().
      setMaster(master).
      setAppName(user + " " + this.getClass.getSimpleName)

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc
  }

}
