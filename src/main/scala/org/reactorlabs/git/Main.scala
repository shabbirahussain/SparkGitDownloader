package org.reactorlabs.git

import java.nio.file.{Path, Paths}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.reactorlabs.git.models.Languages
import org.reactorlabs.git.parsers.GHTParser

import scala.io.Source

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
    util.deleteRecursively(Paths.get(opPath).toFile)

    val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

    val proj = getFilteredProjList(csvPath, parser, blacklistPath = blacklistPath)
      .map(x=> x._1 + "," + x._2)
      .coalesce(1, shuffle=false)
      .saveAsTextFile(opPath)
  }

  /** Parses and gets the list of projects from GHTorrent file. Removes projects from languages other than specified.
    * Also removes blacklisted projects. If blacklist file is provided.
    *
    * @param csvPath is the path of GHTorrent CSV file.
    * @param parser is the GHTParser to be used.
    * @param blacklistPath is the path of the blacklist file to use. (Optional)
    * @return RDD of (projId, (projLangID, isDeleted, isForked, isCorrput, createdTime))
    */
  def getFilteredProjList(csvPath: String, parser: Broadcast[GHTParser], blacklistPath: String = null):
    RDD[(String, Long)] = {

    var res = spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
      .map(parser.value.parse)
      .rdd
      .filter(!_._2._2) // isDeleted == false
      .filter(!_._2._3) // isForked  == false
      .filter(!_._2._4) // isCorrput == false
      .reduceByKey((u, v) => if(u._5 > v._5) u else v) // Keep the latest record
      .mapValues(_._5)

    // Try filtering out blacklist files.
    try{
      val blacklist = sc.broadcast(Source.fromFile(blacklistPath)
        .getLines.toStream
        .map(x=> x.hashCode)
        .toSet)

      res = res.filter(x => !blacklist.value.contains(x._1.hashCode))
      blacklist.destroy()
    } catch {case _: Exception => System.err.println("No blacklist detected.")}
    res
  }
}
