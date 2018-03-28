package org.reactorlabs.jshealth.analysis

import org.reactorlabs.jshealth.Main.{prop, sc, spark}

object Analysis {
  import org.apache.spark.sql.{SQLContext, SparkSession, Column, DataFrame}
  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.sql

  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  val sqlContext: SQLContext = spark.sqlContext
  import sqlContext.implicits._
  sc.setCheckpointDir("/Users/shabbirhussain/Data/project/mysql-2018-02-01/temp/")
  sc.setLogLevel("ERROR")

  val customSchema = StructType(Array(
    StructField("REPO_OWNER",   StringType, nullable = false),
    StructField("REPOSITORY",   StringType, nullable = false),
    StructField("GIT_PATH",     StringType, nullable = false),
    StructField("HASH_CODE",    StringType, nullable = true),
    StructField("COMMIT_TIME",  LongType,   nullable = false)))

  val rawData = sqlContext.read.format("csv").
    option("delimiter",",").option("quote","\"").schema(customSchema).
    load("/Users/shabbirhussain/Google Drive/NEU/Notes/CS 8678 [Project]/Déjà vu: Episode II - Attack of The Clones/Data/FILE_HASH_HISTORY/").distinct

  val allData = rawData.
    filter($"REPO_OWNER".isNotNull && $"REPOSITORY".isNotNull && $"GIT_PATH".isNotNull &&$"COMMIT_TIME".isNotNull).
    withColumn("HASH_CODE", when($"HASH_CODE" === "\\N", null).otherwise($"HASH_CODE")).
    select($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH", $"HASH_CODE", $"COMMIT_TIME".cast(sql.types.LongType)).
    withColumn("min(COMMIT_TIME)", min("COMMIT_TIME").over(Window.partitionBy("HASH_CODE"))).
    repartition($"REPO_OWNER", $"REPOSITORY", $"GIT_PATH").
    withColumn("HEAD_COMMIT_TIME", first("COMMIT_TIME").over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME".desc))).
    withColumn("HEAD_HASH_CODE"  , first("HASH_CODE"  ).over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME".desc))).
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
      $"HEAD_COMMIT_TIME".as("O_HEAD_COMMIT_TIME"),
      $"HEAD_HASH_CODE"  .as("O_HEAD_HASH_CODE")
    )

  // List all the copied content (hash equal).
  val copy = allData.
    filter($"COMMIT_TIME" =!= $"min(COMMIT_TIME)" && $"HASH_CODE".isNotNull).drop($"min(COMMIT_TIME)").
    join(orig, usingColumn = "HASH_CODE").
    filter( // Prevent file revert getting detected as copy
      $"REPO_OWNER" =!= $"O_REPO_OWNER" ||
      $"REPOSITORY" =!= $"O_REPOSITORY" ||
      $"GIT_PATH"   =!= $"O_GIT_PATH"
    ).
    filter(!($"O_HEAD_HASH_CODE".isNull && $"O_HEAD_COMMIT_TIME" === $"COMMIT_TIME")). // Ignore immediate moves
    checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER)

  // Count unique files in the head.
  val (allUniqPathsCount, origUniqPathsCount, copyUniqPathsCount) = {

    /* Special case when two files are together checkedin both unique. So both of them become original. Now one file is deleted and commited again.
     * This makes this path as copy coz file was deleted and now this path with higher timerstamp gets connected to other original.

    // allData.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").show
    // orig.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").show
    // copy.filter($"GIT_PATH" === "client/node_modules/azure/node_modules/xmlbuilder/lib/XMLBuilder.js").orderBy("COMMIT_TIME").take(1)
    */

    val allUniqPathsCount = allData.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
      filter($"HASH_CODE".isNotNull).                       // Paths which aren't deleted
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
    val origUniqPathsCount = orig.
      filter($"O_COMMIT_TIME" === $"O_HEAD_COMMIT_TIME").   // Head only
      filter($"HASH_CODE".isNotNull).                       // Paths which aren't deleted
      select("O_REPO_OWNER", "O_REPOSITORY", "O_GIT_PATH").distinct.count
    val copyUniqPathsCount = copy.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").       // Head only
      filter($"HASH_CODE".isNotNull).                       // Paths which aren't deleted
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count

    (allUniqPathsCount, origUniqPathsCount, copyUniqPathsCount)
  }

  // Divergent Analysis
  val divergentCopyCount = copy.select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.
    join(orig.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Head only
      filter($"HASH_CODE".isNotNull)                    // Paths which aren't deleted
      , joinExprs =
        $"O_REPO_OWNER" === $"REPO_OWNER" &&
        $"O_REPOSITORY" === $"REPOSITORY" &&
        $"O_GIT_PATH"   === $"GIT_PATH"
    ).count


  // Obsolete code analysis
  val activeRepoObsoleteCopyCount = {
    // List of all the copied content which is at the head of that path.
    val undeletedCopiesAtHead = copy.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME"). // Head only
      filter($"HASH_CODE".isNotNull)              // Paths which aren't deleted

    val repoLastCommitTime = allData.
      groupBy("REPO_OWNER", "REPOSITORY").max("COMMIT_TIME").
      withColumnRenamed("max(COMMIT_TIME)", "REPO_LAST_COMMIT_TIME")

    // Repos which have a later commit than bug. (Only active js development.)
    // TODO: will moving a file count towards obsolete code?
    // TODO: Merge following two commands
    val activeRepoObsoleteCopyCount = undeletedCopiesAtHead.
      filter($"O_HEAD_HASH_CODE" =!= $"HASH_CODE"). // Specifies original was fixed after
      // filter($"O_FIX_HASH_CODE".isNotNull).      // Specifies original was fixed after and the fix wasn't a delete
      join(repoLastCommitTime, Seq("REPO_OWNER", "REPOSITORY")).
      withColumn("IS_ACTIVE", $"REPO_LAST_COMMIT_TIME" > $"O_HEAD_COMMIT_TIME").
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "IS_ACTIVE").distinct.
      groupBy("IS_ACTIVE").count.collect

    activeRepoObsoleteCopyCount
  }

  // Copy as Import
  val copyAsImportCount = {
    val w1 = Window.partitionBy("REPO_OWNER", "REPOSITORY", "FOLDER").orderBy($"COMMIT_TIME")
    // Get all the paths fingerprint present in the all repo upto a max(original commit point) used by the copier.

    val allJSFilesAtTimeOfCopyFromSrc = allData.
      withColumn("crc32(GIT_PATH)" , crc32($"GIT_PATH")).
      withColumn("crc32(HASH_CODE)", crc32($"HASH_CODE")).
      withColumn("crc32(PREV_HASH_CODE)", lag("crc32(HASH_CODE)", 1).
        over(Window.partitionBy("REPO_OWNER", "REPOSITORY", "GIT_PATH").orderBy($"COMMIT_TIME"))).
      // Create flags to identify commit type for a path.
      withColumn("IS_ADDITION", $"crc32(PREV_HASH_CODE)".isNull).
      withColumn("IS_DELETION", $"crc32(HASH_CODE)".isNull).
      // Null value replace checksums of hash. We will treat them as zero in further calculations.
      withColumn("crc32(HASH_CODE)"     , when($"IS_DELETION", 0).otherwise($"crc32(HASH_CODE)")).
      withColumn("crc32(PREV_HASH_CODE)", when($"IS_ADDITION", 0).otherwise($"crc32(PREV_HASH_CODE)")).
      // Overcome double counting for the same path. This way we are only considering new additions.
      withColumn("crc32(GIT_PATH)",
        when($"IS_ADDITION",  $"crc32(GIT_PATH)").
        when($"IS_DELETION", -$"crc32(GIT_PATH)").
        otherwise(0)).
      withColumn("crc32(HASH_CODE)", $"crc32(HASH_CODE)" - $"crc32(PREV_HASH_CODE)").drop("crc32(PREV_HASH_CODE)").
      // Extract first level folder to aggregate repository.
      withColumn("FOLDER", split($"GIT_PATH", "/")).withColumn("FOLDER", when(size($"FOLDER") === 1, "").otherwise($"FOLDER"(0))).
      // Fingerprint all additions in a commit for a whole repo.
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "FOLDER").
      agg(sum($"crc32(GIT_PATH)"), sum($"crc32(HASH_CODE)"), sum(signum($"crc32(GIT_PATH)"))).
      // Do cumulative sum of fingerprints.
      select(
        $"REPO_OWNER", $"REPOSITORY", $"COMMIT_TIME", $"FOLDER",
        sum($"sum(crc32(GIT_PATH))").over(w1).as("sum(crc32(GIT_PATH))"),
        sum($"sum(crc32(HASH_CODE))").over(w1).as("sum(crc32(HASH_CODE))"),
        sum($"sum(signum(crc32(GIT_PATH)))").over(w1).as("sum(count(GIT_PATH))")
      ).
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Copy path resembles original path
    val truePathCopy = copy.
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").                                       // Pick head paths only.
      filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").   // They should have a different repo to compare
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
      agg(max($"O_COMMIT_TIME")).
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Cross join all paths fingerprint within a copied folder with all the orig repos form that contributed to this folder.
    val allCopyPathsXRepo = copy.select("REPO_OWNER", "REPOSITORY", "GIT_PATH", "COMMIT_TIME", "HASH_CODE").
      distinct.           // Using distinct as multiple originals can be present as 2 copies committed simultaneously
      join(truePathCopy, usingColumns = Seq("REPO_OWNER", "REPOSITORY", "COMMIT_TIME")).
      filter($"GIT_PATH".startsWith($"GIT_PATH_PREFIX")).
      withColumn("O_GIT_PATH", $"GIT_PATH".substr(length($"GIT_PATH_PREFIX") + 1, lit(100000))).drop("GIT_PATH").
      withColumn("crc32(HASH_CODE)" , crc32($"HASH_CODE")).
      withColumn("crc32(O_GIT_PATH)", crc32($"O_GIT_PATH")).
      // Extract first level folder to aggregate repository.
      withColumn("O_FOLDER", split($"O_GIT_PATH", "/")).withColumn("O_FOLDER", when(size($"O_FOLDER") === 1, "").otherwise($"O_FOLDER"(0))).
      groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "max(O_COMMIT_TIME)", "O_FOLDER").
      agg(sum("crc32(HASH_CODE)"), sum("crc32(O_GIT_PATH)"), count("O_GIT_PATH")).
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Do join to find if all the paths from original at the time of copy exists in the copied folder.
    val copyAsImportExamples = allCopyPathsXRepo.
      join(allJSFilesAtTimeOfCopyFromSrc.
        select(
          $"REPO_OWNER".as("O_REPO_OWNER"),
          $"REPOSITORY".as("O_REPOSITORY"),
          $"COMMIT_TIME".as("O_COMMIT_TIME"),
          $"FOLDER".as("O_FOLDER"),
          $"sum(crc32(HASH_CODE))",
          $"sum(crc32(GIT_PATH))" .as("sum(crc32(O_GIT_PATH))"),
          $"sum(count(GIT_PATH))" .as("count(O_GIT_PATH)")
        ),
        usingColumns = Seq("O_REPO_OWNER", "O_REPOSITORY", "O_FOLDER", "sum(crc32(O_GIT_PATH))", "sum(crc32(HASH_CODE))", "count(O_GIT_PATH)")).
      filter($"max(O_COMMIT_TIME)" <= $"O_COMMIT_TIME").
      select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY", "count(O_GIT_PATH)").distinct.
      checkpoint(true).
      persist(StorageLevel.MEMORY_AND_DISK_SER)


    // TODO: What should we consider that path if it belongs to copy as import and then changed to something else?
    val copyAsImportCount = copyAsImportExamples.agg(sum("count(O_GIT_PATH)")).collect




  copyAsImportCount
  }

/*

    copy.filter($"GIT_PATH".contains("node_modules/")).
      filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Pick head paths only.
      filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
      filter($"GIT_PATH".contains($"O_GIT_PATH")).
      select("REPO_OWNER", "REPOSITORY", "GIT_PATH").distinct.count
      join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").withColumn("JNK", lit(true)).distinct,
        usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
        joinType = "LEFT_OUTER").
      filter($"JNK".isNull).count

      val test = copy.
        filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").   // Pick head paths only.
        filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").
        filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
        filter($"GIT_PATH".contains($"O_GIT_PATH")).
        join(copyAsImportExamples.select("REPO_OWNER", "REPOSITORY").withColumn("JNK", lit(true)).distinct,
          usingColumns = Seq("REPO_OWNER", "REPOSITORY"),
          joinType = "LEFT_OUTER").
        filter($"JNK".isNull).persist(StorageLevel.MEMORY_AND_DISK_SER)

      test.filter($"GIT_PATH".contains("node_modules/")).count
      test.filter($"GIT_PATH".contains("node_modules/") && $"O_GIT_PATH".contains("node_modules/")).count

      test.filter(!$"GIT_PATH".contains("node_modules/")).show


      copy.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com" && $"GIT_PATH".contains("irhydra/2.bak/packages/core_elements/src/web-animations-next/node_modules/mocha/")).
        filter($"COMMIT_TIME" === $"HEAD_COMMIT_TIME").                                       // Pick head paths only.
        filter($"REPO_OWNER" =!= $"O_REPO_OWNER" || $"REPOSITORY" =!= $"O_REPOSITORY").   // They should have a different repo to compare
        filter($"GIT_PATH".contains($"O_GIT_PATH")).
        filter(length($"GIT_PATH") > length($"O_GIT_PATH")).
        withColumn("GIT_PATH_PREFIX", $"GIT_PATH".substr(lit(0), length($"GIT_PATH") - length($"O_GIT_PATH"))).
        groupBy("REPO_OWNER", "REPOSITORY", "COMMIT_TIME", "GIT_PATH_PREFIX", "O_REPO_OWNER", "O_REPOSITORY").
        agg(max($"O_COMMIT_TIME")).show


    truePathCopy.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com" && $"GIT_PATH_PREFIX".contains("irhydra/2.bak/packages/core_elements/src/web-animations-next/node_modules/mocha/")).show
            truePathCopy.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli").show


    allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha").show

    allCopyPathsXRepo.filter($"REPO_OWNER" === "mraleph" && $"REPOSITORY" === "mraleph.github.com").show




      allData.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha" && !$"GIT_PATH".contains("/")).show


    allData.filter($"REPO_OWNER" === "mochajs" && $"REPOSITORY" === "mocha" && !$"GIT_PATH".contains("/")).
      rdd.coalesce(1, true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")

            allCopyPathsXRepo.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli").show


            allData.filter($"REPO_OWNER" === "oscmejia" && $"REPOSITORY" === "tutorial-nodejs-cli" && $"GIT_PATH".contains("lib/commander.js")).show


            allData.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js" && ).show
            allData.filter($"HASH_CODE" === "cdd206a9d678c529fe6ec3f44483e8a90368c8ec").agg(min("COMMIT_TIME")).collect

            allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js").show


            allData.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" ===  "commander.js" && $"COMMIT_TIME" <= "1344026849").
              orderBy($"GIT_PATH", $"COMMIT_TIME").
              select("GIT_PATH", "HASH_CODE", "COMMIT_TIME").
              rdd.coalesce(1, true).saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/debug")


            allJSFilesAtTimeOfCopyFromSrc.filter($"REPO_OWNER" === "tj" && $"REPOSITORY" === "commander.js").show



        */


  // Chain copy res = 0
//  {
//    val connect = copy.
//      withColumn("C", concat($"REPO_OWNER", lit("/"), $"REPOSITORY")).
//      withColumn("O", concat($"O_REPO_OWNER", lit("/"), $"O_REPOSITORY")).
//      select("O", "C").checkpoint(true).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    val cc = connect.as("A").join(connect.as("B"), joinExprs = $"A.O" === $"B.C")
//
//
//
//
//  }


  // Phase of copy
//    val testTime = df.filter(lower($"GIT_PATH").contains("test/")).
//      groupBy($"REPO_OWNER", $"REPOSITORY").
//      agg(min("COMMIT_TIME")).withColumnRenamed("min(COMMIT_TIME)", "T_COMMIT_TIME").
//      checkpoint(true)
//
////    val copyPhase = df.groupBy($"REPO_OWNER", $"REPOSITORY").
////      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), count("COMMIT_TIME"), countDistinct("GIT_PATH")).
////      join(copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME"), usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
////      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
////      join(testTime, usingColumns = Seq("REPO_OWNER", "REPOSITORY"), joinType = "LEFT_OUTER").
////      withColumn("T_COMMIT_TIME1", when($"T_COMMIT_TIME".isNull, $"max(COMMIT_TIME)").otherwise($"T_COMMIT_TIME")).
////      withColumn("TEST_PHASE", ($"T_COMMIT_TIME1" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)")).
////      groupBy("count(COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(mean("COPY_PHASE"), mean("TEST_PHASE")).
////      checkpoint(true)
//
//    //val copy2 = copy.select("REPO_OWNER", "REPOSITORY", "COMMIT_TIME")
//    val copy3 = copy.groupBy("REPO_OWNER", "REPOSITORY").min("COMMIT_TIME").withColumnRenamed("min(COMMIT_TIME)", "COMMIT_TIME")
//    val copyPhase = allData.groupBy($"REPO_OWNER", $"REPOSITORY").
//      agg(min("COMMIT_TIME"), max("COMMIT_TIME"), countDistinct("COMMIT_TIME"), countDistinct("GIT_PATH")).
//      join(copy3, usingColumns = Seq("REPO_OWNER", "REPOSITORY")).
//      withColumn("COPY_PHASE", ($"COMMIT_TIME" - $"min(COMMIT_TIME)")/($"max(COMMIT_TIME)" - $"min(COMMIT_TIME)" + 1)).
//      groupBy("count(DISTINCT COMMIT_TIME)", "count(DISTINCT GIT_PATH)").agg(sum("COPY_PHASE"), count("COPY_PHASE")).
//      checkpoint(true)
//
//    copyPhase.rdd.map(_.mkString(",")).
//      coalesce(1, shuffle = true).
//      saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/report/copyPhase")


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
}
