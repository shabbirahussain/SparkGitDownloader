package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main.{getNewDBConnection, sc, spark, sqlContext, dbConnOptions}
import org.reactorlabs.jshealth.models.FileHashTuple

/**
  * @author shabbirahussain
  */
class LocalStore() extends DataStore {
  import sqlContext.implicits._

  /** Executes batch of sql statements.
    *
    * @param sqls is the rdd of sql statements.
    * @param batchSize is the size of the batches to use.
    */
  private def execInBatch(sqls: RDD[String], batchSize: Int= 1000): Unit = {
    //sqls.foreach(println)
    sqls
      .mapPartitions(_.grouped(batchSize))
      .foreach(batch => {
        val connection = getNewDBConnection
        val statement = connection.createStatement()
        batch.foreach(sql =>statement.addBatch(sql))
        statement.executeBatch()
      })
  }

  // ================ API Methods ===================
  override def markReposCompleted(token: Long, errorRepo: RDD[(String, String)] = null): Unit = {
    val connection = getNewDBConnection
    connection
      .createStatement
      .executeLargeUpdate(
        """
          |UPDATE REPOS_QUEUE
          |   SET COMPLETED   = TRUE,
          |       CHECKOUT_ID = NULL
          |WHERE CHECKOUT_ID  = '%d';
        """.stripMargin.format(token)) // todo: Unsafe. prone to sql injection

    // Update errors to the database.
//    if (errorRepo != null){
//      execInBatch(errorRepo
//        .map(row =>{
//            val split = row._1.split(":")
//              """
//                |UPDATE REPOS_QUEUE
//                |   SET RESULT = %s
//                |WHERE REPOSITORY   = '%s'
//                |  AND BRANCH       = '%s';
//              """.stripMargin.format(row._2, split(0), split(1))
//          })
//      )
//    }
  }

  override def checkoutReposToCrawl(limit: Int = 1000): (RDD[String], Long) = {
    val rdd = spark.sqlContext
      .read
      .format("jdbc")
      .options(dbConnOptions)
      .option("dbtable", "REPOS_QUEUE")
      .load()
      .select($"REPOSITORY", $"BRANCH")
      .filter($"COMPLETED" === false)
      .filter($"CHECKOUT_ID" isNull)
      .limit(limit)
      .rdd

    val token = System.currentTimeMillis()

    // Set checkout timestamp
    execInBatch(
      rdd.map(row =>{
            """
              |UPDATE REPOS_QUEUE
              |   SET CHECKOUT_ID = %d
              |WHERE REPOSITORY   = '%s'
              |  AND BRANCH       = '%s';
            """.stripMargin.format(token, row.get(0), row.get(1))
        })
    )

    (rdd.map[String](x=> x.get(0) + "/" + x.get(1) + ":"), token)
  }

  override def store(fht: RDD[FileHashTuple]): Unit = {
    execInBatch(fht
      .map(row => {
            """
              |INSERT INTO FILE_HASH_HEAD(BRANCH, GIT_PATH, HASH_CODE)
              |VALUES ('%s', '%s', '%s');
            """.stripMargin.format(row.branch, row.url, row.fileHash)
        })
    )
  }

  override def storeHistory(fht: RDD[FileHashTuple]): Unit = {
    execInBatch(fht
      .map(row => {
        """
          |INSERT INTO FILE_HASH_HISTORY(BRANCH, GIT_PATH, HASH_CODE, COMMIT_ID, COMMIT_TIME)
          |VALUES ('%s', '%s', '%s', '%s', '%d');
        """.stripMargin.format(row.branch, row.url, row.fileHash, row.commitId, row.commitTime)
      }))
  }

  override def loadProjectsQueue(projects: RDD[String], flushExisting: Boolean): Unit = {
    if (flushExisting) { // Truncate table
      val connection = getNewDBConnection
      connection
        .createStatement
        .execute("TRUNCATE TABLE REPOS_QUEUE;")
    }
    println("projs=", projects.count())

    execInBatch(projects
      .map(row => {
          """
            |INSERT INTO REPOS_QUEUE(REPOSITORY)
            |VALUES ('%s');
          """.stripMargin.format(row)
      }))
  }
}
