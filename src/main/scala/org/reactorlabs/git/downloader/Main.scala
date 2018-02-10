package org.reactorlabs.git.downloader

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.reactorlabs.git.downloader.models.Languages
import org.reactorlabs.git.downloader.parsers.GHTParser

object Main {
  val spark = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
//  Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)
  sc.setCheckpointDir("target/temp/spark/")


  def main(args: Array[String]): Unit = {
    val csvPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects_full.csv"
    val opPath = csvPath + "-spark/"
    util.deleteRecursively(Paths.get(opPath).toFile)

    val parser = new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript))

    getFilteredProjList(csvPath, parser)
      .map(x=> x._1 + "," + x._2._1 + "," + x._2._5)
      .coalesce(1, shuffle=false)
      .saveAsTextFile(opPath)
  }

  def getFilteredProjList(csvPath: String, parser: GHTParser):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {

    spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
      .map(parser.parse)
      .rdd
      .filter(!_._2._2) // isDeleted == false
      .filter(!_._2._3) // isForked  == false
      .filter(!_._2._4) // isCorrput == false
      .reduceByKey((u, v) => if(u._5 > v._5) u else v) // Keep the latest record
  }
}
