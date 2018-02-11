package org.reactorlabs.git

import java.nio.file.Paths

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.reactorlabs.git.Main.{sc, spark}
import org.reactorlabs.git.ghtorrent.models.Languages
import sys.process._

package object ghtorrent {
  val ghtRepoPath: String = "/Users/shabbirhussain/Data/project"
  val gitRepoPath: String = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/git"
  val name: String = "mysql-2018-02-01"
  val blacklistPath = "/Users/shabbirhussain/Data/project/mysql-2018-02-01/blacklist.csv"
  val filters = Set("filterDeleted", "filterForked", "filterExisting", "filterBlacklist", "filterTop100000")
  val ghtDownloaderPath = "/Users/shabbirhussain/Documents/workspace/Project/SparkGitDownloader/src/main/shell/GHTorrent.sh"

  private val ghtProjectsFile  = ghtRepoPath + "/" + name + "/Projects.csv"
  private val ghtProjectsCache = ghtRepoPath + "/" + name + "/Projects.cache"
  private val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

  import spark.implicits._

  /** Loads the project list with appropriate filters as specified in the configuration.
    *
    * @return a RDD of project urls.
    */
  def getProjectList(): RDD[String] = {
    if (!Paths.get(ghtProjectsCache).toFile.exists()){
      if (!Paths.get(ghtProjectsFile).toFile.exists()){
        downloadProjectsList(name + ".tar.gz")
      }
      saveProjectList(loadRawProjectList(ghtProjectsFile, parser), ghtProjectsCache)
    }
    applyFilters(loadCachedProjectList(ghtProjectsCache))
  }


  /**
    * Downloads projects.csv from GHTorrent.
    */
  private def downloadProjectsList(): Unit = {
    val cmd = ghtDownloaderPath + " " + name + ".tar.gz " + ghtRepoPath + " " + name + "/projects.csv"
    println("$ " + cmd)
    cmd !
  }

  /** Parses and gets the list of projects from GHTorrent file.
    *
    * @param csvPath is the path of GHTorrent CSV file.
    * @param parser is the GHTParser to be used.
    * @return RDD of projId.
    */
  private def loadRawProjectList(csvPath: String, parser:  Broadcast[GHTParser]):
    RDD[String] = {
    var res = spark.read
      .option("header", "true")       // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .textFile(csvPath)
      .map(parser.value.parse)
      .rdd

    res = ProjectFilters.filterCorrupt(res)
    if (filters.contains("filterDeleted"))  res = ProjectFilters.filterDeleted(res)
    if (filters.contains("filterForked"))   res = ProjectFilters.filterForked(res)

    val n = (filters + "filterTop-1").filter(_.startsWith("filterTop"))
        .map(x=>try {x.substring(9).toInt} catch{case _:Exception=> -1}).max
    if (n > -1)                             res = ProjectFilters.filterTopN(res, n)

    res.map(_._1)
  }

  /** Parses and gets the list of projects from GHTorrent file.
    *
    * @param serFilePath is the path of pre-processed GHTorrent CSV file.
    * @return RDD of projId.
    */
  private def loadCachedProjectList(serFilePath: String):
    RDD[String] = {
    spark.read.textFile(serFilePath).rdd
  }

  private def saveProjectList(rdd: RDD[String], serFilePath: String): Unit = {
    util.deleteRecursively(Paths.get(ghtProjectsCache).toFile)
    rdd.saveAsTextFile(serFilePath)
  }

  /** Applies filters to the loaded RDD of projects.
    *
    * @param rdd is the RDD of the projUrls.
    * @return an RDD after applying filters.
    */
  private def applyFilters(rdd: RDD[String]):
    RDD[String] = {
    var res = rdd
    if (filters.contains("filterExisting")) res = ProjectFilters.filterExisting(res, gitRepoPath)
    if (filters.contains("filterBlacklist"))res = ProjectFilters.filterBlacklist(res, blacklistPath)
    res
  }
}
