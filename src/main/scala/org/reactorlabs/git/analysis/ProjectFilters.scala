package org.reactorlabs.git.analysis

import java.nio.file.Paths

import org.apache.spark.rdd.RDD

import scala.io.Source

/** Applies filters to the given projects list.
  *
  * @param filters is the Set of filters to apply to the given rdd.
  * @param repoPath is the base location of the repository.
  * @param blacklistPath is the path of the blacklist file to use.
  *
  * @author shabbir.ahussain
  */
class ProjectFilters(filters: Set[String], repoPath: String, blacklistPath: String) {
  private val repoDir = Paths.get(repoPath).toFile
  if (! repoDir.exists()) repoDir.mkdirs()

  def applyFilters(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[String] = {
    var res = filterCorrupt(rdd)
    if (filters.contains("filterDeleted"))  res = filterDeleted(res)
    if (filters.contains("filterForked"))   res = filterForked(res)
    if (filters.contains("filterExisting")) res = filterExisting(res)
    if (filters.contains("filterBlacklist"))res = filterBlacklist(res)

    res.map(_._1)
  }

  private def filterCorrupt(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._3)
  }

  private def filterDeleted(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._2)
  }

  private def filterForked(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._3)
  }

  private def filterExisting(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(x=> Paths.get(repoDir.getAbsolutePath + x._1).toFile.exists)
  }

  private def filterBlacklist(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    try{
      val blacklist = Source.fromFile(blacklistPath)
        .getLines.toStream
        .map(x=> x.hashCode)
        .toSet

    if (blacklist.nonEmpty)
      return rdd.filter(x => !blacklist.contains(x.hashCode))
    } catch {case _: Exception => System.err.println("No blacklist detected.")}
    rdd
  }
}
