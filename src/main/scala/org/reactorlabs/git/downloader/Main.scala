package org.reactorlabs.git.downloader

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.reactorlabs.git.downloader.parsers.GHTParser
import org.reactorlabs.git.downloader.models.Languages
import org.apache.spark.sql.functions.rank

object Main {
  val spark = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")
  sc.setCheckpointDir("target/temp/spark/")

  def main(args: Array[String]): Unit = {
    val csvPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects1000.csv"
    val opPath = csvPath + "-spark/"



    val parser = new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript))

    val projects = spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
      .map(parser.parse)
      .toDF()


    val filtered = projects
      .filter($"isDeleted" === false)
      .filter($"isCorrupt" === false)
      .withColumn("rank", rank.over(Window.partitionBy($"projUrl").orderBy($"created" desc)))
//      .filter($"rank" === 1 )
      .rdd
      .coalesce(1, shuffle=false)
      .saveAsTextFile(opPath)
  }
}
