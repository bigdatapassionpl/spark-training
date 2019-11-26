package com.bigdatapassion.spark.streaming

import com.bigdatapassion.spark.core.BaseSparkApp

object HdfsStreaming extends BaseSparkStreamingApp with BaseSparkApp {

  def main(args: Array[String]) {

    val ssc = createStreamingContext

    val lines = ssc.textFileStream(bookPath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
