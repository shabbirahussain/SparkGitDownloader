package org.reactorlabs.jshealth.ghtorrent

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermission

import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{ds, logger, prop, sc, spark}
import org.reactorlabs.jshealth.models.Languages

import sys.process._
import scala.language.postfixOps
import scala.util.Try
/**
  * @author shabbirahussain
  */
object Main {
  private val ghtDownloaderPath = prop.getProperty("ghtorrent.downloader.path")
  private val ghtTarName        = prop.getProperty("ghtorrent.archive.name")
  private val ghtProjFilters    = prop.getProperty("ghtorrent.project.filters").split(",").map(_.trim).toSet

  private val ghtRepoPath      = "%s/ght/".format(sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp"))
  private val filesToDownload  = Seq("projects.csv").map(x=> "%s/%s".format(ghtTarName, x))
  private val ghtProjectsFile  = "%s%s/%s" .format(ghtRepoPath, ghtTarName, "projects.csv")
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
      filesToDownload.mkString(" ")

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
  : RDD[(String, String)] = {
    var res = sc.textFile(csvPath)
      .map(parser.value.parse)
      .filter(_.isSuccess)
      .map(_.get)
      .toDF()

    if (ghtProjFilters.contains("filterDeleted")) res = ProjectFilters.filterDeleted(res)
    if (ghtProjFilters.contains("filterForked"))  res = ProjectFilters.filterForked(res)

    ghtProjFilters
      .filter(_.startsWith("filterTop"))
      .map(x=> Try(x.substring(9).toInt))
      .filter(_.isSuccess).map(_.get)
      .take(1)
      .foreach(n=> res = ProjectFilters.filterTopN(res, n))

     res.rdd
       .map(row=> (row.getAs[String]("projOwner"), row.getAs[String]("projRepo")))
       .distinct()
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
