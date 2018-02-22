package org.reactorlabs.jshealth.analysis

import org.apache.spark.SparkContext
import org.reactorlabs.jshealth.Main.{prop, spark}



//  val windowSpec = Window.partitionBy($"HASH_CODE").
//    orderBy($"COMMIT_TIME".asc)
//  val ranked = rdd.
//    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME",
//    rank().over(windowSpec).as("RANK")).
//    persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//  val orig = rdd.filter($"RANK" === 1)
//  val repl = rdd.filter($"RANK" =!= 1)

object Analysis {
  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.sql

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  val dbConnOptions = Map("driver" -> "com.mysql.cj.jdbc.Driver",
    "url" -> "jdbc:mysql://localhost/reactorlabs_2018_02_01?autoReconnect=true&useSSL=false&maxReconnects=100",
    "username"  -> "reactorlabs",
    "user"      -> "reactorlabs",
    "password"  -> "gthe123",
    "schema"    -> "reactorlabs_2018_02_01")

  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._
  spark.sparkContext.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")


  val df = sqlContext.read.format("jdbc").options(dbConnOptions).
//    option("dbtable", "TEMP").load().
        option("dbtable", "FILE_HASH_HISTORY").load().
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val joinedDf = df.join(df.groupBy("HASH_CODE").min("COMMIT_TIME"), usingColumn = "HASH_CODE").
    persist(StorageLevel.MEMORY_AND_DISK_SER_2)


  // List all original content based on first commit.
  val orig = joinedDf.where($"COMMIT_TIME" === $"min(COMMIT_TIME)").drop($"COMMIT_TIME").
    checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  // List all the copied content (hash equal).
  val copy = joinedDf.as("J").where($"COMMIT_TIME" =!= $"min(COMMIT_TIME)").drop($"min(COMMIT_TIME)").
    join(orig.as("O"), "HASH_CODE").
    // Prevent file revert getting detected as copy
    filter($"J.REPO_OWNER" =!= $"O.REPO_OWNER" || $"J.REPOSITORY" =!= $"O.REPOSITORY" || $"J.GIT_PATH" =!= $"O.GIT_PATH").
    select($"J.REPO_OWNER", $"J.REPOSITORY", $"J.GIT_PATH", $"HASH_CODE", $"J.COMMIT_TIME").distinct

  // List of all the copied content which is at the head of that path.
  val headHashOfCopy = joinedDf.as("J").groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").max("COMMIT_TIME").
    join(copy.as("C"), usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"max(COMMIT_TIME)" === $"C.COMMIT_TIME").
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "C.COMMIT_TIME", "HASH_CODE").
    checkpoint(true)


  // List of the original content which has modifications later on.
  val bugFixedHash = orig.as("OLD").
    join(orig.as("NEW"), usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"OLD.min(COMMIT_TIME)" < $"NEW.min(COMMIT_TIME)").
    groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"OLD.HASH_CODE", $"OLD.min(COMMIT_TIME)").
    min("NEW.min(COMMIT_TIME)").
    withColumnRenamed("min(COMMIT_TIME)", "O_FIRST_COMMIT_TIME").
    withColumnRenamed("min(min(COMMIT_TIME))", "O_FIX_COMMIT_TIME").
    checkpoint(true)

  // Join of headHashOfCopy and bugFixedHash codes.
  val buggy = headHashOfCopy.as("H").join(bugFixedHash.as("F"), "HASH_CODE").
    select($"H.REPO_OWNER", $"H.REPOSITORY", $"H.GIT_PATH", $"H.COMMIT_TIME", $"F.REPO_OWNER".as("O_REPO_OWNER"), $"F.REPOSITORY".as("O_REPOSITORY"), $"F.GIT_PATH".as("GIT_PATH"), $"F.O_FIRST_COMMIT_TIME", $"F.O_FIX_COMMIT_TIME").
    checkpoint(true)


  // Repos which have a later commit than bug. (Only active js development.
  val activeRepoBugs = joinedDf.groupBy("REPO_OWNER", "REPOSITORY").max("COMMIT_TIME").
    withColumnRenamed("max(COMMIT_TIME)", "REPO_LAST_COMMIT_TIME").
    join(buggy, Seq("REPO_OWNER", "REPOSITORY")).
    filter($"REPO_LAST_COMMIT_TIME" > $"O_FIX_COMMIT_TIME").
    checkpoint(true)

  activeRepoBugs.show(10)



  def main(args: Array[String]): Unit = {
  }




//  df.groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").max("COMMIT_TIME").filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").show(100)
//
//  orig.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("min(COMMIT_TIME)").show(100)
//  copy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)
//  bugFixedHash.filter($"O_REPO_OWNER" === "3rd-Eden").filter($"O_GIT_PATH" === "test/common.js").orderBy("O_COMMIT_TIME").show(100)
//  headHashOfCopy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)
//
//  buggy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)
//  buggy.show(1000)


}
