package org.reactorlabs.git

import java.nio.file.Paths

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.reactorlabs.git.models.Languages
import org.reactorlabs.git.parsers.GHTParser
import org.reactorlabs.git.analysis.ProjectFilters


/**
  * @author shabbir.ahussain
  */
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
    val csvPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects1000000.csv"
    val blacklistPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/blacklist.csv"
    val opPath = csvPath + "-spark/"
    val repoPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/gitImage/"
    util.deleteRecursively(Paths.get(opPath).toFile)

    val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

    val filters = new ProjectFilters(
      Set("filterDeleted", "filterForked", "filterExisting", "filterBlacklist"),
      repoPath = repoPath,
      blacklistPath = blacklistPath)


    val rddp = getProjectList(csvPath, parser)
    val rddf = filters.applyFilters(rddp)

    rddf.coalesce(1, shuffle=false)
      .saveAsTextFile(opPath)
  }

  /** Parses and gets the list of projects from GHTorrent file.
    *
    * @param csvPath is the path of GHTorrent CSV file.
    * @param parser is the GHTParser to be used.
    * @return RDD of (projId, (projLangID, isDeleted, isForked, isCorrput, createdTime))
    */
  def getProjectList(csvPath: String,
                     parser:  Broadcast[GHTParser]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
      .map(parser.value.parse)
      .rdd
  }
}
