package org.reactorlabs.jshealth.ghtorrent

import org.apache.spark.rdd.RDD

import scala.io.Source

/** Applies filters to the given projects list.
  *
  * @author shabbirahussain
  */
object ProjectFilters{
  def filterCorrupt(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._4)
  }

  def filterDeleted(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._2)
  }

  def filterForked(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))]):
    RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.filter(!_._2._3)
  }

  def filterTopN(rdd: RDD[(String, (Int, Boolean, Boolean, Boolean, Long))], n: Int):
  RDD[(String, (Int, Boolean, Boolean, Boolean, Long))] = {
    rdd.zipWithIndex.filter(_._2 < n).map(x=> x._1)
  }

  def filterBlacklist(rdd: RDD[String], blacklistPath: String):
    RDD[String] = {
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
