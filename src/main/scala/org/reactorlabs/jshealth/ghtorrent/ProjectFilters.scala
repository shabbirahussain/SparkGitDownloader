package org.reactorlabs.jshealth.ghtorrent

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.reactorlabs.jshealth.Main.spark

import scala.io.Source

/** Applies filters to the given projects list.
  *
  * @author shabbirahussain
  */
object ProjectFilters{
  import spark.implicits._

  def filterDeleted(df: DataFrame): DataFrame = {
    df.filter(!$"isDeleted")
  }

  def filterForked(df: DataFrame): DataFrame = {
    df.filter(!$"isForked")
  }

  def filterTopN(df: DataFrame, n: Int): DataFrame = {
    df.limit(n)
  }
}
