package org.reactorlabs.jshealth.analysis

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.reactorlabs.jshealth.Main.{prop, sc, spark}


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
  sc.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")
  sc.setLogLevel("ERROR")

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
    join(orig.
      select($"REPO_OWNER".as("O_REPO_OWNER"),
        $"REPOSITORY".as("O_REPOSITORY"),
        $"GIT_PATH".as("O_GIT_PATH"),
        $"min(COMMIT_TIME)".as("O_COMMIT_TIME"),
        $"HASH_CODE").as("O"),
      "HASH_CODE").
    // Prevent file revert getting detected as copy
    filter($"J.REPO_OWNER" =!= $"O.O_REPO_OWNER" || $"J.REPOSITORY" =!= $"O.O_REPOSITORY" || $"J.GIT_PATH" =!= $"O.O_GIT_PATH").
    select("J.REPO_OWNER", "J.REPOSITORY", "J.GIT_PATH", "J.COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH", "O_COMMIT_TIME", "HASH_CODE").
    checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)


  // List of all the copied content which is at the head of that path.
  val head = joinedDf.groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").max("COMMIT_TIME").
    checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val headHashIsCopy = head.
    join(copy.select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME").distinct
      .as("C"), usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"max(COMMIT_TIME)" === $"C.COMMIT_TIME").
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "C.COMMIT_TIME", "HASH_CODE")
  val headHashOfOrig = head.
    join(orig.as("O"), usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
    where($"max(COMMIT_TIME)" === $"O.min(COMMIT_TIME)").
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "O.min(COMMIT_TIME)", "HASH_CODE")


  // Obsolete code analysis
  val (uniqPaths, origUniqPaths, copyUniqPaths) = {
    // List of the original content which has modifications later on.
    val updatedOrigHash = orig.as("OLD").
      join(orig.as("NEW"), usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
      where($"OLD.min(COMMIT_TIME)" < $"NEW.min(COMMIT_TIME)").
      groupBy($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"OLD.HASH_CODE", $"OLD.min(COMMIT_TIME)").
      min("NEW.min(COMMIT_TIME)").
      withColumnRenamed("min(COMMIT_TIME)", "O_FIRST_COMMIT_TIME").
      withColumnRenamed("min(min(COMMIT_TIME))", "O_FIX_COMMIT_TIME").
      checkpoint(true)

    // Join of headHashOfCopy and bugFixedHash codes.
    val obsoleteCopy = headHashIsCopy.as("H").
      join(updatedOrigHash.as("F"), "HASH_CODE").
      select($"H.REPO_OWNER", $"H.REPOSITORY", $"H.GIT_PATH", $"H.COMMIT_TIME", $"F.REPO_OWNER".as("O_REPO_OWNER"), $"F.REPOSITORY".as("O_REPOSITORY"), $"F.GIT_PATH".as("O_GIT_PATH"), $"F.O_FIRST_COMMIT_TIME", $"F.O_FIX_COMMIT_TIME").
      checkpoint(true)

    // Repos which have a later commit than bug. (Only active js development.)
    val activeRepoObsoleteCopy = joinedDf.groupBy("REPO_OWNER", "REPOSITORY").max("COMMIT_TIME").
      withColumnRenamed("max(COMMIT_TIME)", "REPO_LAST_COMMIT_TIME").
      join(obsoleteCopy, Seq("REPO_OWNER", "REPOSITORY")).
      filter($"REPO_LAST_COMMIT_TIME" > $"O_FIX_COMMIT_TIME").
      checkpoint(true)

    val uniqPaths = df.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val origUniqPaths = orig.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val copyUniqPaths = copy.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    //  activeRepoObsoleteCopy.show(10)
    //  activeRepoObsoleteCopy.rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/activeBugs.csv")

    (uniqPaths, origUniqPaths, copyUniqPaths)
  }

  // Divergent Analysis
  val (nonDivergentCopy, divergentCopyCount)  = {
    val nonDivergentCopy = headHashIsCopy.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.withColumn("JNK", lit(true))
    val copy2 = copy.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.checkpoint(true)
    val divergentCopy    = copy2.
      join(nonDivergentCopy,
        usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH"), joinType = "LEFT_OUTER").
      withColumn("IS_DIVERGENT", $"JNK".isNull).drop("JNK").checkpoint(true)
    val divergentCopyCount = divergentCopy.groupBy("IS_DIVERGENT").count.collect()


    //  val divergentAllCommmits = divergentCopy.filter($"IS_DIVERGENT").drop("IS_DIVERGENT", "min(COMMIT_TIME)").
    //    join(joinedDf, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).as("ALL").checkpoint(true)
    //  val divergentExample = divergentAllCommmits.
    //    join(copy, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH", "HASH_CODE", "COMMIT_TIME"), joinType = "LEFT_OUTER").
    //    orderBy("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME").
    //    checkpoint(true)

    //  divergentExample.filter($"GIT_PATH" === "lib/ripple/platform/wac/1.0/spec/config.js" && $"REPO_OWNER" === "01org").show(10)
    //  joinedDf.filter($"GIT_PATH" === "lib/ripple/platform/wac/1.0/spec/config.js" && $"REPO_OWNER" === "01org").take(100)(0)
    (nonDivergentCopy, divergentCopyCount)
  }

  // Trying to keep updated
  val nonDivergentCopySyncedMoreThanOnceCount = {
    val copySyncedMoreThanOnce = copy.
      groupBy("REPO_OWNER", "REPOSITORY", "GIT_PATH", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH").
      agg(countDistinct("HASH_CODE")).
      filter($"count(DISTINCT HASH_CODE)" > 1).drop("count(DISTINCT HASH_CODE)")

    val nonDivergentCopySyncedMoreThanOnce = nonDivergentCopy.drop("JNK").
      join(copySyncedMoreThanOnce, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
      join(headHashOfOrig.select($"REPO_OWNER".as("O_REPO_OWNER"), $"REPOSITORY".as("O_REPOSITORY"), $"GIT_PATH".as("O_GIT_PATH")).withColumn("JNK", lit(true)),
        usingColumns = Seq("O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH"), joinType = "LEFT_OUTER").
      withColumn("IS_IN_SYNC", $"JNK".isNotNull).drop("JNK").
      checkpoint(true)

    val nonDivergentCopySyncedMoreThanOnceCount = nonDivergentCopySyncedMoreThanOnce.
      groupBy("IS_IN_SYNC").count.collect()
    nonDivergentCopySyncedMoreThanOnceCount
  }



  // Copy as Import
  {
    val truePathCopy = copy.filter($"GIT_PATH".contains($"O_GIT_PATH")).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val temp = truePathCopy.
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY").
      agg(max("O_COMMIT_TIME"), countDistinct("GIT_PATH")).
      withColumnRenamed("count(DISTINCT GIT_PATH)", "COPY_PATH_COUNT").
      checkpoint(true)

    val allOrigJSFilesAtTimeOfCopy = joinedDf.as("ALL").
      join(temp.as("C"),
      joinExprs =
        $"O_REPO_OWNER" === $"ALL.REPO_OWNER" &&
          $"O_REPOSITORY" === $"ALL.REPOSITORY" &&
          $"max(O_COMMIT_TIME)" >= $"ALL.COMMIT_TIME").
      groupBy("C.REPO_OWNER", "C.REPOSITORY", "C.COMMIT_TIME", "C.O_REPO_OWNER", "C.O_REPOSITORY", "C.COPY_PATH_COUNT").
      agg(countDistinct("GIT_PATH")).
      filter($"count(DISTINCT GIT_PATH)" === $"COPY_PATH_COUNT").
      select($"C.REPO_OWNER",
        $"C.REPOSITORY",
        $"C.COMMIT_TIME",
        $"C.O_REPO_OWNER",
        $"C.O_REPOSITORY").
      checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val copyAsImportCount = truePathCopy.join(allOrigJSFilesAtTimeOfCopy.withColumn("JNK", lit(true)),
      usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY"),
      joinType = "LEFT_OUTER").withColumn("IS_COPY_AS_IMPORT", $"JNK".isNotNull).
      groupBy("IS_COPY_AS_IMPORT").count.collect


    val test = truePathCopy.join(allOrigJSFilesAtTimeOfCopy.withColumn("JNK", lit(true)),
      usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY"),
      joinType = "LEFT_OUTER").withColumn("IS_COPY_AS_IMPORT", $"JNK".isNotNull).checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    copyAsImportCount
  }


  // Chain copy res = 0
  {
    val connect = copy.
      withColumn("C", $"REPO_OWNER" + "/" + $"REPOSITORY").
      withColumn("O", $"O_REPO_OWNER" + "/" + $"O_REPOSITORY").
      select("O", "C").checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val cc = connect.as("A").joinWith(connect.as("B"), $"A.O" === $"B.C")
  }


  // Phase of copy
  {
    val testTime = df.filter(lower($"GIT_PATH").contains("test/")).
      groupBy($"REPO_OWNER", $"REPOSITORY").
      agg(min("COMMIT_TIME")).withColumnRenamed("min(COMMIT_TIME)", "T_COMMIT_TIME").
      checkpoint(true)

//    val copyPhase = df.groupBy($"REPO_OWNER", $"REPOSITORY").
//      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), count("COMMIT_TIME"), countDistinct("GIT_PATH")).
//      join(copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME"), usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
//      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
//      join(testTime, usingColumns = Seq("REPO_OWNER", "REPOSITORY"), joinType = "LEFT_OUTER").
//      withColumn("T_COMMIT_TIME1", when($"T_COMMIT_TIME".isNull, $"max(COMMIT_TIME)").otherwise($"T_COMMIT_TIME")).
//      withColumn("TEST_PHASE", ($"T_COMMIT_TIME1" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
//      groupBy("count(COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(mean("COPY_PHASE"), mean("TEST_PHASE")).
//      checkpoint(true)

    //val copy2 = copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME")
    val copy3 = copy.groupBy("REPO_OWNER", "REPOSITORY").min("COMMIT_TIME").withColumnRenamed("min(COMMIT_TIME)", "COMMIT_TIME")
    val copyPhase = joinedDf.groupBy($"REPO_OWNER", $"REPOSITORY").
      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), countDistinct("COMMIT_TIME"), countDistinct("GIT_PATH")).
      join(copy3, usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)" + 1)).
      groupBy("count(DISTINCT COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(sum("COPY_PHASE"), count("COPY_PHASE")).
      checkpoint(true)

    copyPhase.rdd.map(_.mkString(",")).
      coalesce(1, shuffle = true).
      saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/copyPhase")

    copyPhase.show(10)

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

//  B creates a files
  //
  // copies from A
}
