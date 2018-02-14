package org.reactorlabs.jshealth

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * @author shabbir.ahussain
  */
object Main {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")
  sc.setCheckpointDir("target/temp/spark/")


  val prop = new Properties()
  try {
    val stream = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    prop.load(stream)
    if (stream != null) stream.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val logger = Logger.getLogger("project.default.logger")
  def main(args: Array[String]): Unit = {
    //ghtorrent.getProjectList()
    git.exec
  }
}
