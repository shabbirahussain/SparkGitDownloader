package org.reactorlabs.git

import java.nio.file.Paths

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.reactorlabs.git.ghtorrent.models.Languages
import org.reactorlabs.git.ghtorrent.{GHTParser, ProjectFilters}


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




    ghtorrent.getProjectList()
  }


}
