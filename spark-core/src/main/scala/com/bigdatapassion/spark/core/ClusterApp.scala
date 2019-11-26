package com.bigdatapassion.spark.core

trait ClusterApp {

  /**
    * User name
    */
  val user: String = "bigdata"

  /**
    * Where to run master process
    */
  val master: String = "yarn"

  /**
    * Directory path with sample data
    */
  val dataPath: String = "/bigdatapassion"

  /**
    * Output path (results)
    */
  val resultPath: String = "/user/" + user + "/wyniki/spark"

}
