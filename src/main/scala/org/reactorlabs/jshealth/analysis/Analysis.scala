package org.reactorlabs.jshealth.analysis

import java.util

import org.apache.spark.SparkContext
import org.reactorlabs.jshealth.Main.{prop, sc, spark}

object Analysis {
  import org.apache.spark.sql.{SQLContext, SparkSession, Column, DataFrame}
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.sql

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  val dbConnOptions = Map("driver" -> "com.mysql.cj.jdbc.Driver",
    "url" -> "jdbc:mysql://localhost/reactorlabs_2018_02_01?serverTimezone=UTC&autoReconnect=true&useSSL=false&maxReconnects=10",
    "username"  -> "reactorlabs",
    "user"      -> "reactorlabs",
    "password"  -> "gthe123",
    "schema"    -> "reactorlabs_2018_02_01")

  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._
  sc.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")
  sc.setLogLevel("ERROR")

  val allData = sqlContext.read.format("jdbc").options(dbConnOptions).
//    option("dbtable", "TEMP").
    option("dbtable", "FILE_HASH_HISTORY").
    load().
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    withColumn("min(COMMIT_TIME)", min("COMMIT_TIME").over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH"))).
    repartition($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").
    withColumn("FIX_COMMIT_TIME", lead("COMMIT_TIME", 1).over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy("COMMIT_TIME"))).
    withColumn("FIX_HASH_CODE"  , lead("HASH_CODE"  , 1).over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy("COMMIT_TIME"))).
    checkpoint(true).
    persist(StorageLevel.MEMORY_AND_DISK_SER)

  // List all original content based on first commit.
  val orig = allData.
    filter($"COMMIT_TIME" === $"min(COMMIT_TIME)" && $"HASH_CODE".isNotNull).
    select(
      $"REPO_OWNER" .as("O_REPO_OWNER"),
      $"REPOSITORY" .as("O_REPOSITORY"),
      $"GIT_PATH"   .as("O_GIT_PATH"),
      $"COMMIT_TIME".as("O_COMMIT_TIME"),
      $"HASH_CODE",
      $"FIX_COMMIT_TIME".as("O_FIX_COMMIT_TIME"),
      $"FIX_HASH_CODE"  .as("O_FIX_HASH_CODE")
    ).checkpoint(true).persist(StorageLevel.DISK_ONLY)

  // List all the copied content (hash equal).
  val copy = allData.where($"COMMIT_TIME" =!= $"min(COMMIT_TIME)" && $"HASH_CODE".isNotNull).drop($"min(COMMIT_TIME)").
    join(orig, usingColumn = "HASH_CODE").
    // Prevent file revert getting detected as copy
    filter(
      $"REPO_OWNER" =!= $"O_REPO_OWNER" ||
      $"REPOSITORY" =!= $"O_REPOSITORY" ||
      $"GIT_PATH"   =!= $"O_GIT_PATH"       // TODO: When should we consider copy within a repo?
    ).filter($"O_FIX_HASH_CODE".isNotNull). // Ignore immediate moves
    checkpoint(true).persist(StorageLevel.DISK_ONLY)

  // Obsolete code analysis
  val (uniqPaths, origUniqPaths, copyUniqPaths, activeRepoObsoleteCopyCount, divergentUniqPaths) = {
    // List of all the copied content which is at the head of that path.
    val copiesAtHead = copy.filter($"FIX_COMMIT_TIME".isNull)

    // Repos which have a later commit than bug. (Only active js development.)
    // TODO: will moving a file count towards obsolete code?
    val activeRepoObsoleteCopy = allData.
      groupBy("REPO_OWNER", "REPOSITORY").max("COMMIT_TIME").
      withColumnRenamed("max(COMMIT_TIME)", "REPO_LAST_COMMIT_TIME").
      join(copiesAtHead.
          filter($"O_FIX_COMMIT_TIME".isNotNull). // Specifies original was fixed after
          filter($"O_FIX_HASH_CODE".isNotNull),   // The fix wasn't a delete
        Seq("REPO_OWNER", "REPOSITORY")).
      withColumn("IS_ACTIVE", $"REPO_LAST_COMMIT_TIME" > $"O_FIX_COMMIT_TIME").
      checkpoint(true)

    val uniqPaths     = allData.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val origUniqPaths = orig.select("O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH").distinct.count
    val copyUniqPaths = copiesAtHead.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val divergentUniqPaths = copyUniqPaths - copyUniqPaths
    val activeRepoObsoleteCopyCount = activeRepoObsoleteCopy.groupBy("IS_ACTIVE").count.collect

    //  activeRepoObsoleteCopy.show(10)
    //  activeRepoObsoleteCopy.rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/activeBugs.csv")

    (uniqPaths, origUniqPaths, copyUniqPaths, activeRepoObsoleteCopyCount, divergentUniqPaths)
  }

/*  // Divergent Analysis
  val (nonDivergentCopy, divergentCopyCount)  = {


    val nonDivergentCopy = copiesAtHead.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.withColumn("JNK", lit(true))
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

    // Trying to keep updated

//    val nonDivergentCopySyncedMoreThanOnceCount = {
//      val copySyncedMoreThanOnce = copy.
//        groupBy("REPO_OWNER", "REPOSITORY", "GIT_PATH", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH").
//        agg(countDistinct("HASH_CODE")).
//        filter($"count(DISTINCT HASH_CODE)" > 1).drop("count(DISTINCT HASH_CODE)")
//
//      val nonDivergentCopySyncedMoreThanOnce = nonDivergentCopy.drop("JNK").
//        join(copySyncedMoreThanOnce, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH")).
//        join(headHashOfOrig.select($"REPO_OWNER".as("O_REPO_OWNER"), $"REPOSITORY".as("O_REPOSITORY"), $"GIT_PATH".as("O_GIT_PATH")).withColumn("JNK", lit(true)),
//          usingColumns = Seq("O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH"), joinType = "LEFT_OUTER").
//        withColumn("IS_IN_SYNC", $"JNK".isNotNull).drop("JNK").
//        checkpoint(true)
//
//      val nonDivergentCopySyncedMoreThanOnceCount = nonDivergentCopySyncedMoreThanOnce.
//        groupBy("IS_IN_SYNC").count.collect()
//      nonDivergentCopySyncedMoreThanOnceCount
//    }


    (nonDivergentCopy, divergentCopyCount)
  }*/


  // Copy as Import
  val copyAsImportCount = {
    // Copy path resembles original path
    val truePathCopy = copy.//filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").
      filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
      agg(max($"O_COMMIT_TIME")).
      distinct.
      checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val allCopyPathsXRepo = copy.as("C").select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME").
      distinct.     // Multiple originals can be present as 2 copies committed simultaneously
      join(truePathCopy.as("B"),
        joinExprs =
          $"C.REPO_OWNER"  === $"B.REPO_OWNER"  &&
          $"C.REPOSITORY"  === $"B.REPOSITORY"  &&
          $"C.COMMIT_TIME" === $"B.COMMIT_TIME" &&
          $"C.GIT_PATH".startsWith($"B.GIT_PATH_PREFIX")).
      select(
        $"C.REPO_OWNER",
        $"C.REPOSITORY",
        $"GIT_PATH_PREFIX",
        $"C.COMMIT_TIME",
        $"O_REPO_OWNER",
        $"O_REPOSITORY",
        $"C.GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000)).as("O_GIT_PATH")
      ).checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER)


    val allJSFilesAtTimeOfCopyFromSrc = allData.as("ALL").
      join(truePathCopy.as("B"),
        joinExprs =
          $"O_REPO_OWNER" === $"ALL.REPO_OWNER" &&
          $"O_REPOSITORY" === $"ALL.REPOSITORY" &&
          $"max(O_COMMIT_TIME)" >= $"ALL.COMMIT_TIME").
      withColumn("RANK", rank().over(Window.partitionBy($"ALL.REPO_OWNER", $"ALL.REPOSITORY", $"ALL.O_GIT_PATH").orderBy($"ALL.COMMIT_TIME".desc))).
      filter($"RANK" === 1).filter($"HASH_CODE".isNotNull).
      select(
        $"ALL.REPO_OWNER",
        $"ALL.REPOSITORY",
        $"ALL.GIT_PATH".as("O_GIT_PATH"),
        $"B.GIT_PATH_PREFIX",
        $"B.COMMIT_TIME",
        $"B.O_REPO_OWNER",
        $"B.O_REPOSITORY").
      distinct


    val windowSpec = Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH_PREFIX", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY")
    val copyPrefixesWithUnmatchedSrcFiles = allCopyPathsXRepo.withColumn("JNK", lit(false)).
      join(allJSFilesAtTimeOfCopyFromSrc,
        usingColumns = Seq("REPO_OWNER", "REPOSITORY", "GIT_PATH_PREFIX", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH"),
        joinType = "RIGHT_OUTER").
      withColumn("IS_COPIED_AS_IMPORT", count("JNK").over(windowSpec) === count(lit(true)).over(windowSpec)).
      filter($"IS_COPIED_AS_IMPORT").drop("JNK", "IS_COPIED_AS_IMPORT").
      checkpoint(true)


    // TODO: What should we consider that path if it belongs to copy as import and then changed to something else?
    val copyAsImportCount = copyPrefixesWithUnmatchedSrcFiles.select("REPO_OWNER", "REPOSITORY", "GIT_PATH_PREFIX", "O_GIT_PATH").distinct.count

    copyAsImportCount
//    copyPrefixesWithUnmatchedSrcFiles.filter($"REPO_OWNER" === "ajhino" && $"REPOSITORY" === "ajhino.github.com" && $"COMMIT_TIME" === "1344316781").show(1000)
//    copy.filter($"REPO_OWNER" === "ajhino" && $"REPOSITORY" === "ajhino.github.com" && $"COMMIT_TIME" === "1344316781").show(1000)
//    copyPrefixesWithUnmatchedSrcFiles.groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME").count.orderBy($"COUNT").show
  }


  // Chain copy res = 0
  {
    val connect = copy.
      withColumn("C", concat($"REPO_OWNER", lit("/"), $"REPOSITORY")).
      withColumn("O", concat($"O_REPO_OWNER", lit("/"), $"O_REPOSITORY")).
      select("O", "C").checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val cc = connect.as("A").join(connect.as("B"), joinExprs = $"A.O" === $"B.C")




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
    val copyPhase = allData.groupBy($"REPO_OWNER", $"REPOSITORY").
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


  /** Gets the head record from the dataFrame given.
    *
    * @param dataFrame is the source dataFrame.
    * @param usingColumns is the columns on which partitioning is computed to calculate latest record.
    * @param orderBy is the column on which ordering is performed. (Desc order by default)
    * @return An dataframe containing only heads from the given dataFrame.
    */
  def getHead(dataFrame: DataFrame, usingColumns: Seq[Column], orderBy: Column) : DataFrame = {
    val windowSpec = Window.partitionBy(usingColumns:_*).orderBy(orderBy.desc)
    dataFrame.withColumn("RANK", rank().over(windowSpec)).
      filter($"RANK" === 1).
      drop("RANK")
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
