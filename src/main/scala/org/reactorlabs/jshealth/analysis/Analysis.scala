package org.reactorlabs.jshealth.analysis

import org.apache.spark.SparkContext
import org.reactorlabs.jshealth.Main.{prop, spark}

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

  val df = sqlContext.read.format("jdbc").options(dbConnOptions).
    option("dbtable", "TEMP").load().
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    persist(StorageLevel.MEMORY_AND_DISK_SER_2)


  //  val windowSpec = Window.partitionBy($"HASH_CODE").
  //    orderBy($"COMMIT_TIME".asc)
  //  val ranked = rdd.
  //    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME",
  //    rank().over(windowSpec).as("RANK")).
  //    persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  //
  //  val orig = rdd.filter($"RANK" === 1)
  //  val repl = rdd.filter($"RANK" =!= 1)

  spark.sparkContext.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")


  val joinedDf = df.join(df.groupBy("HASH_CODE").min("COMMIT_TIME"), usingColumn = "HASH_CODE").
    persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  // List all original content based on first commit.
  val orig = joinedDf.where($"COMMIT_TIME" === $"min(COMMIT_TIME)").drop($"COMMIT_TIME").checkpoint(true)

  // List all the copied content (hash equal).
  val copy = joinedDf.where($"COMMIT_TIME" =!= $"min(COMMIT_TIME)").drop($"min(COMMIT_TIME)").
    withColumnRenamed("REPO_OWNER"  , "C_REPO_OWNER").
    withColumnRenamed("REPOSITORY"  , "C_REPOSITORY").
    withColumnRenamed("GIT_PATH"    , "C_GIT_PATH").
    withColumnRenamed("COMMIT_TIME" , "C_COMMIT_TIME").
    join(orig, "HASH_CODE").
    filter($"C_REPO_OWNER" =!= $"REPO_OWNER" || $"C_REPOSITORY" =!= $"REPOSITORY" || $"C_GIT_PATH" =!= $"GIT_PATH"). // Prevent file revert getting detected as copy
    withColumnRenamed("HASH_CODE"   , "C_HASH_CODE").
    select("C_REPO_OWNER", "C_REPOSITORY", "C_GIT_PATH", "C_HASH_CODE", "C_COMMIT_TIME").
    checkpoint(true)

  // List of the original content which has modifications later on.
  val bugFixedHash = orig.groupBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").max("min(COMMIT_TIME)").
    join(orig, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"min(COMMIT_TIME)" < $"max(min(COMMIT_TIME))").
    withColumnRenamed("REPO_OWNER"  , "O_REPO_OWNER").
    withColumnRenamed("REPOSITORY"  , "O_REPOSITORY").
    withColumnRenamed("GIT_PATH"    , "O_GIT_PATH").
    withColumnRenamed("min(COMMIT_TIME)"      , "O_FIRST_COMMIT_TIME").
    withColumnRenamed("max(min(COMMIT_TIME))" , "O_UPDT_COMMIT_TIME").
    checkpoint(true)

  // List of all the copied content which is at the head of that path.
  val headHashOfCopy = df.groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").max("COMMIT_TIME").
    join(copy, $"REPO_OWNER" === $"C_REPO_OWNER" && $"REPOSITORY" === $"C_REPOSITORY" && $"GIT_PATH" === $"C_GIT_PATH").
    where($"max(COMMIT_TIME)" === $"C_COMMIT_TIME").
    select("C_REPO_OWNER", "C_REPOSITORY", "C_GIT_PATH", "C_COMMIT_TIME", "C_HASH_CODE").
    checkpoint(true)

  // Join of headHashOfCopy and bugFixedHash codes.
  val buggy = headHashOfCopy.join(bugFixedHash, $"HASH_CODE" === $"C_HASH_CODE").
    select("C_REPO_OWNER", "C_REPOSITORY", "C_GIT_PATH", "C_COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH", "O_FIRST_COMMIT_TIME", "O_UPDT_COMMIT_TIME").checkpoint(true)




  df.groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").max("COMMIT_TIME").filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").show(100)

  orig.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("min(COMMIT_TIME)").show(100)
  copy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)
  bugFixedHash.filter($"O_REPO_OWNER" === "3rd-Eden").filter($"O_GIT_PATH" === "test/common.js").orderBy("O_COMMIT_TIME").show(100)
  headHashOfCopy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)

  buggy.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("COMMIT_TIME").show(100)
  buggy.show(1000)







  orig.filter($"REPO_OWNER" === "3rd-Eden").filter($"GIT_PATH" === "test/common.js").orderBy("min(COMMIT_TIME)").show(100)
  







  orig.groupBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").max("min(COMMIT_TIME)").filter($"REPO_OWNER" === "A21z").filter($"GIT_PATH" === "config.js").orderBy("O_COMMIT_TIME").show(100)


  orig.groupBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").max("min(COMMIT_TIME)").
    join(orig, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"min(COMMIT_TIME)" < $"max(min(COMMIT_TIME))").drop("min(COMMIT_TIME)").
    filter($"REPO_OWNER" === "A21z").filter($"GIT_PATH" === "config.js").orderBy("max(min(COMMIT_TIME))").show(100)

  df.drop("HASH_CODE").join(orig,
    usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"min(COMMIT_TIME)" < $"COMMIT_TIME").
    filter($"REPO_OWNER" === "A21z").filter($"GIT_PATH" === "config.js").orderBy("min(COMMIT_TIME)").show(100)
}
