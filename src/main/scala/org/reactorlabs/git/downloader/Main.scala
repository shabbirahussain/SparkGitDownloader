package org.reactorlabs.git.downloader

import org.apache.spark.sql.SparkSession
import org.reactorlabs.git.downloader.parsers.GHTParser
import org.reactorlabs.git.downloader.models.Languages

object Main {
  val spark = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val csvPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects1000.csv"

    val parser = new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript))

    val projects = spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
//      .map(parser.parse)
        .printSchema()
//      .toDF()

//    projects.printSchema()
  }
}
