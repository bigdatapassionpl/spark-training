package com.bigdatapassion.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.bigdatapassion.spark.core.BaseSparkApp

trait BaseSparkSqlApp extends BaseSparkApp {

  def createSparkSession: SparkSession = {

    val conf = new SparkConf().setAppName(user + " " + this.getClass.getSimpleName)

    SparkSession.builder().
      config(conf).
//      master(master).
      getOrCreate()

  }

}
