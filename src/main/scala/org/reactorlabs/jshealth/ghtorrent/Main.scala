package org.reactorlabs.jshealth.ghtorrent

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermission

import akka.stream.SourceShape
import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{ds, logger, prop, sc, spark}
import org.reactorlabs.jshealth.models.Languages

import scala.io.Source
import sys.process._
import scala.language.postfixOps
/**
  * @author shabbirahussain
  */
object Main {
  private val ghtDownloaderPath = prop.getProperty("ghtorrent.downloader.path")
  private val ghtTarName        = prop.getProperty("ghtorrent.archive.name")
  private val ghtProjFilters    = prop.getProperty("ghtorrent.project.filters").split(",").map(_.trim).toSet

  private val ghtRepoPath      = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp") + "/ght"
  private val ghtProjectsName  = ghtTarName + "/projects.csv"
  private val ghtProjectsFile  = ghtRepoPath + "/" + ghtProjectsName
  private val parser = sc.broadcast(new GHTParser(Set(Languages.JavaScript, Languages.TypeScript, Languages.CoffeeScript)))

  import spark.implicits._

  /** As resource is protected files and aren't executable, this function creates a temporary version of that file.
    *
    * @param resourceName is the resource to clone.
    * @return An absolute path of the clones resource.
    */
  private def cloneResource(resourceName: String)
  : String = {
    val extension = "." + scala.reflect.io.File(scala.reflect.io.Path(resourceName)).extension
    val stream  = this.getClass.getClassLoader.getResourceAsStream(resourceName)
    val f = File.createTempFile(resourceName, extension)
    f.deleteOnExit()
    FileUtils.copyInputStreamToFile(stream, f)
    if (stream != null) stream.close()

    // Make it executable
    val perms = new java.util.HashSet[PosixFilePermission]()
    perms.add(PosixFilePermission.OWNER_EXECUTE)
    perms.add(PosixFilePermission.OWNER_READ)
    Files.setPosixFilePermissions(f.toPath, perms)
    f.getAbsolutePath
  }

  /**
    * Downloads projects.csv from GHTorrent.
    */
  private def downloadProjectsList()
  : Unit = {
    // Create a temp sh script
    val cmd = cloneResource(ghtDownloaderPath) + " " +
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
  private def getProjectList(csvPath: String, parser: Broadcast[GHTParser])
  : RDD[String] = {
    var res = sc.textFile(csvPath).map(parser.value.parse)

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
    logger.log(Level.INFO, "GHT archive folder = %s".format(ghtRepoPath))

    if (!Paths.get(ghtProjectsFile).toFile.exists()){
      logger.log(Level.INFO, "No project.csv found. Downloading from GHTorrent...")
      downloadProjectsList()
    }
    ds.storeProjectsQueue(getProjectList(ghtProjectsFile, parser))
  }
}
