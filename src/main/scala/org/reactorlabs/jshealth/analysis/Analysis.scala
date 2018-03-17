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
    "url" -> "jdbc:mysql://localhost/reactorlabs_2018_02_01?serverTimezone=UTC&autoReconnect=true&useSSL=false&maxReconnects=10",
    "username"  -> "reactorlabs",
    "user"      -> "reactorlabs",
    "password"  -> "gthe123",
    "schema"    -> "reactorlabs_2018_02_01")

  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._
  sc.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")
  sc.setLogLevel("ERROR")

  val joinedDf = sqlContext.read.format("jdbc").options(dbConnOptions).
    option("dbtable", "TEMP").
//    option("dbtable", "FILE_HASH_HISTORY").
    load().
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    withColumn("min(COMMIT_TIME)", min("COMMIT_TIME").over(Window.partitionBy("HASH_CODE"))).
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
    select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH", "O_COMMIT_TIME", "HASH_CODE").
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


    val truePathCopy = copy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
      agg(max($"O_COMMIT_TIME")).
      distinct.
      checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//    val truePathCopyXRepo = copy.as("C").select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH", "HASH_CODE").
//      distinct.     // Multiple originals can be present as 2 copies committed simultaneously
//      join(truePathCopy.as("B"),
//        joinExprs =
//          $"C.REPO_OWNER"  === $"B.REPO_OWNER" &&
//          $"C.REPOSITORY"  === $"B.REPOSITORY" &&
//          $"C.COMMIT_TIME" === $"B.COMMIT_TIME" &&
//          $"C.GIT_PATH".startsWith($"B.GIT_PATH_PREFIX")).
//      select($"C.REPO_OWNER", $"C.REPOSITORY", $"C.COMMIT_TIME", $"HASH_CODE".as("O_HASH_CODE"), $"O_REPO_OWNER", $"O_REPOSITORY",
//        $"C.GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000)).as("O_GIT_PATH")).
//      checkpoint(true).
//      persist(StorageLevel.MEMORY_AND_DISK_SER_2)


//    allJSFilesAtTimeOfCopyFromSrc.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas").
//    truePathCopyXRepo.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas").


    val allCopyPathsXRepo = copy.as("C").select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH").
      distinct.     // Multiple originals can be present as 2 copies committed simultaneously
      join(truePathCopy.as("B"),
        joinExprs =
          $"C.REPO_OWNER"  === $"B.REPO_OWNER"  &&
          $"C.REPOSITORY"  === $"B.REPOSITORY"  &&
          $"C.COMMIT_TIME" === $"B.COMMIT_TIME" &&
          $"C.GIT_PATH".startsWith($"B.GIT_PATH_PREFIX")).
      select($"C.REPO_OWNER", $"C.REPOSITORY", $"C.COMMIT_TIME", $"O_REPO_OWNER", $"O_REPOSITORY", $"GIT_PATH_PREFIX",
        $"C.GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000)).as("O_GIT_PATH")).
      checkpoint(true).
      persist(StorageLevel.MEMORY_AND_DISK_SER_2)


    val allJSFilesAtTimeOfCopyFromSrc = joinedDf.as("ALL").
      join(truePathCopy.as("B"),
        joinExprs =
          $"O_REPO_OWNER" === $"ALL.REPO_OWNER" &&
          $"O_REPOSITORY" === $"ALL.REPOSITORY" &&
          $"max(O_COMMIT_TIME)" >= $"ALL.COMMIT_TIME").
      select($"B.REPO_OWNER", $"B.REPOSITORY", $"B.COMMIT_TIME", $"B.O_REPO_OWNER", $"B.O_REPOSITORY", $"B.GIT_PATH_PREFIX", $"ALL.GIT_PATH".as("O_GIT_PATH")).
      distinct.
      checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val copyPrefixesWithUnmatchedSrcFiles = allCopyPathsXRepo.withColumn("JNK", lit(false)).
      join(allJSFilesAtTimeOfCopyFromSrc,
        usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "GIT_PATH_PREFIX", "O_GIT_PATH"),
        joinType = "RIGHT_OUTER").
      filter($"JNK".isNull).
      select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "GIT_PATH_PREFIX").distinct.
      checkpoint(true).
      persist(StorageLevel.MEMORY_AND_DISK_SER_2)


    val allCopiesNotImportedAsCopy = allCopyPathsXRepo.join(copyPrefixesWithUnmatchedSrcFiles,
      usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY", "GIT_PATH_PREFIX")).
      checkpoint(true)

//    val totalCopyCount = truePathCopyXRepo.count
    val copiesNotImportedAsCopyCount = allCopiesNotImportedAsCopy.count


    //      agg(countDistinct("GIT_PATH")).
    //      filter($"count(DISTINCT GIT_PATH)" === $"COPY_PATH_COUNT").
    //      select($"C.REPO_OWNER",
    //        $"C.REPOSITORY",
    //        $"C.COMMIT_TIME",
    //        $"C.O_REPO_OWNER",
    //        $"C.O_REPOSITORY").

//    joinedDf.filter($"COMMIT_TIME" <= "1340566101" && $"REPO_OWNER" === "aFarkas" && $"REPOSITORY" === "webshim").select("GIT_PATH").distinct.count
//
//    copyPrefixesWithUnmatchedSrcFiles.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas").
//    allJSFilesAtTimeOfCopyFromSrc.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").
//      rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")
//
//    allCopyPathsXRepo.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").
//      rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")

//
//
//    val copyAsImportCount = truePathCopy.join(allOrigJSFilesAtTimeOfCopy.withColumn("JNK", lit(true)),
//      usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "O_REPO_OWNER", "O_REPOSITORY"),
//      joinType = "LEFT_OUTER").withColumn("IS_COPY_AS_IMPORT", $"JNK".isNotNull).
//      groupBy("IS_COPY_AS_IMPORT").count.collect
//
//
//
//    allCopyPathsXRepo.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")
//    truePathCopy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").count
//    allCopyPathsXRepo.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").select($"O_REPO_OWNER", $"O_REPOSITORY").count
//
//
//    truePathCopy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").select($"O_REPO_OWNER", $"O_REPOSITORY").distinct.show(10)
//    allCopyPathsXRepo.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate").select($"O_REPO_OWNER", $"O_REPOSITORY").distinct.show(10)
//    truePathCopy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas" && $"O_REPOSITORY" === "webshim").show(10)
//    temp.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas" && $"O_REPOSITORY" === "webshim").show(10)
//
//
//    val test = joinedDf.as("ALL").
//      join(temp.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas" && $"O_REPOSITORY" === "webshim").as("C"),
//        joinExprs =
//          $"O_REPO_OWNER" === $"ALL.REPO_OWNER" &&
//            $"O_REPOSITORY" === $"ALL.REPOSITORY" &&
//            $"max(O_COMMIT_TIME)" >= $"ALL.COMMIT_TIME").
//      groupBy("C.REPO_OWNER", "C.REPOSITORY", "C.COMMIT_TIME", "C.O_REPO_OWNER", "C.O_REPOSITORY", "C.COPY_PATH_COUNT").
//      agg(countDistinct("GIT_PATH")).
//      filter($"count(DISTINCT GIT_PATH)" === $"COPY_PATH_COUNT").
//      select($"C.REPO_OWNER",
//        $"C.REPOSITORY",
//        $"C.COMMIT_TIME",
//        $"C.O_REPO_OWNER",
//        $"C.O_REPOSITORY").
//      checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//

//    copy.filter($"REPO_OWNER" === "aFarkas" && $"REPOSITORY" === "webshim" && $"GIT_PATH" === "src/shims/excanvas.js").show(10)
//    copy.filter($"REPO_OWNER" === "aFarkas" && $"REPOSITORY" === "webshim" && $"GIT_PATH" === "src/shims/excanvas.js")
//
//    copy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"GIT_PATH" === "public/aFarkas-webshim/src/shims/excanvas.js")
//    copy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas" && $"O_REPOSITORY" === "webshim" && $"GIT_PATH" === "public/aFarkas-webshim/src/shims/excanvas.js")
//    truePathCopy.filter($"COMMIT_TIME" === "1340894583" && $"REPO_OWNER" === "OC-Git" && $"REPOSITORY" === "LeanTemplate" && $"O_REPO_OWNER" === "aFarkas" && $"O_REPOSITORY" === "webshim" && $"GIT_PATH" === "public/aFarkas-webshim/src/shims/excanvas.js")
//      .rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")
//    test.rdd.coalesce(1, shuffle = true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")


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
