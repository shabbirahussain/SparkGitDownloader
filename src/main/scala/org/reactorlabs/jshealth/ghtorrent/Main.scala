package org.reactorlabs.jshealth.ghtorrent

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermission

import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark, dataStore}
import org.reactorlabs.jshealth.models.Languages

import sys.process._

/**
  * @author shabbirahussain
  */
object Main {
  private val ghtTarName        = prop.getProperty("ghtorrent.archive.name")
  private val ghtRepoPath       = prop.getProperty("ghtorrent.repo.path")
  private val ghtDownloaderPath = prop.getProperty("ghtorrent.downloader.path")
  private val ghtProjFilters    = prop.getProperty("ghtorrent.project.filters").split(",").map(_.trim).toSet

  private val ghtProjectsName  = ghtTarName + "/projects.csv"
  private val ghtProjectsFile  = ghtRepoPath + "/" + ghtProjectsName
  private val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

  import spark.implicits._

  /**
    * Downloads projects.csv from GHTorrent.
    */
  private def downloadProjectsList()
  : Unit = {
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
  private def loadProjectList(csvPath: String, parser:  Broadcast[GHTParser])
  : RDD[String] = {
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

  def main(args: Array[String])
  : Unit = {
    logger.log(Level.INFO, "Starting GHTorrent Process...")

    if (!Paths.get(ghtProjectsFile).toFile.exists()){
      logger.log(Level.INFO, "No project.csv found. Downloading from GHTorrent...")
      downloadProjectsList()
    }
    dataStore.loadProjectsQueue(loadProjectList(ghtProjectsFile, parser))
  }
}
