package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermission

import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark}
import org.reactorlabs.jshealth.ghtorrent.models.Languages

import sys.process._

package object ghtorrent {
  private val ghtTarName        = prop.getProperty("ghtorrent.archive.name")
  private val ghtRepoPath       = prop.getProperty("ghtorrent.repo.path")
  private val ghtDownloaderPath = prop.getProperty("ghtorrent.downloader.path")
  private val gitRepoPath       = prop.getProperty("git.repo.path")
  private val blacklistPath     = prop.getProperty("git.blacklist.path")
  private val ghtProjFilters    = prop.getProperty("ghtorrent.project.filters").split(",").map(_.trim).toSet

  private val ghtProjectsName  = ghtTarName + "/projects.csv"
  private val ghtProjectsFile  = ghtRepoPath + "/" + ghtProjectsName
  private val ghtProjectsCache = ghtRepoPath + "/" + ghtProjectsName + ".cache/"
  private val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

  import spark.implicits._

  /** Loads the project list with appropriate filters as specified in the configuration.
    *
    * @return a RDD of project urls.
    */
  def getProjectList(): RDD[String] = {
    if (!Paths.get(ghtProjectsCache).toFile.exists()){
      logger.log(Level.INFO, "No project cache found. Generating new cache...")
      if (!Paths.get(ghtProjectsFile).toFile.exists()){
        logger.log(Level.INFO, "No project.csv found. Downloading from GHTorrent...")
        downloadProjectsList()
      }
      saveProjectList(loadRawProjectList(ghtProjectsFile, parser), ghtProjectsCache)
    }
    logger.log(Level.INFO, "Loading from project cache and applying filters...")
    applyFilters(loadCachedProjectList(ghtProjectsCache))
  }

  /**
    * Downloads projects.csv from GHTorrent.
    */
  private def downloadProjectsList(): Unit = {
    // Create a temp sh script
    val stream  = this.getClass.getClassLoader.getResourceAsStream(ghtDownloaderPath)
    val ghtFile = File.createTempFile(ghtDownloaderPath, ".sh")
    ghtFile.deleteOnExit()
    FileUtils.copyInputStreamToFile(stream, ghtFile)
    if (stream != null) stream.close()

    // Make it executable
    val perms = new java.util.HashSet[PosixFilePermission]()
    perms.add(PosixFilePermission.OWNER_EXECUTE)
    perms.add(PosixFilePermission.OWNER_READ)
    Files.setPosixFilePermissions(ghtFile.toPath, perms)

    val cmd = ghtFile.getAbsolutePath + " " +
      ghtTarName + ".tar.gz " +
      ghtRepoPath + " " +
      ghtProjectsName

    logger.log(Level.DEBUG, "$ " + cmd)
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
    if (ghtProjFilters.contains("filterDeleted"))  res = ProjectFilters.filterDeleted(res)
    if (ghtProjFilters.contains("filterForked"))   res = ProjectFilters.filterForked(res)

    val n = (ghtProjFilters + "filterTop-1").filter(_.startsWith("filterTop"))
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
    if (ghtProjFilters.contains("filterExisting")) res = ProjectFilters.filterExisting(res, gitRepoPath)
    if (ghtProjFilters.contains("filterBlacklist"))res = ProjectFilters.filterBlacklist(res, blacklistPath)
    res
  }
}
