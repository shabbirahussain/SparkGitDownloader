package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main.{getNewDBConnection, sc, spark, sqlContext, dbConnOptions}
import org.reactorlabs.jshealth.models.FileHashTuple
import org.apache.commons.lang.StringEscapeUtils.escapeSql

/**
  * @author shabbirahussain
  */
class LocalStore(batchSize: Int) extends DataStore {
  import sqlContext.implicits._

  /** Executes batch of sql statements.
    *
    * @param sqls is the rdd of sql statements.
    * @param batchSize is the size of the batches to use.
    */
  private def execInBatch(sqls: RDD[String], batchSize: Int = batchSize): Unit = {
    //sqls.foreach(println)
    sqls
      .mapPartitions(_.grouped(batchSize))
      .foreach(batch => {
        val connection = getNewDBConnection
        val statement  = connection.createStatement()
        batch
          .filter(_ != null)
          .foreach(sql => statement.addBatch(sql))
        statement.executeBatch()
        connection.close()
      })
  }

  // ================ API Methods ===================
  override def markReposCompleted(token: Long, errorRepo: RDD[(FileHashTuple, String)] = null)
  : Unit = {
    val connection = getNewDBConnection
    connection
      .createStatement
      .executeLargeUpdate(
        """
          |UPDATE REPOS_QUEUE
          |   SET COMPLETED   = TRUE,
          |       CHECKOUT_ID = NULL
          |WHERE CHECKOUT_ID  = %d;
        """.stripMargin.format(token))

    // Update errors to the database.
    if (errorRepo != null){
      execInBatch(errorRepo
        .map(row =>{
              """
                |UPDATE REPOS_QUEUE
                |   SET RESULT = %s
                |WHERE REPO_OWNER   = '%s'
                |  AND REPOSITORY   = '%s'
                |  AND BRANCH       = '%s';
              """.stripMargin.format(escapeSql(row._2), row._1.owner, row._1.repo, row._1.branch)
          })
      )
    }
  }

  override def markLinksAsCompleted(token: Long, errorRepo: RDD[(FileHashTuple, String)] = null)
  : Unit = {
    val connection = getNewDBConnection
    connection
      .createStatement
      .executeLargeUpdate(
        """
          |UPDATE FILE_HISTORY_QUEUE
          |   SET COMPLETED   = TRUE,
          |       CHECKOUT_ID = NULL
          |WHERE CHECKOUT_ID  = %d;
        """.stripMargin.format(token))

    // Update errors to the database.
    if (errorRepo != null){
      execInBatch(errorRepo
        .map(row =>{
          """
            |UPDATE REPOS_QUEUE
            |   SET RESULT = %s
            |WHERE OWNER        = '%s'
            |  AND REPOSITORY   = '%s'
            |  AND BRANCH       = '%s';
          """.stripMargin.format(escapeSql(row._2), row._1.owner, row._1.repo, row._1.branch)
        })
      )
    }
  }

  override def checkoutReposToCrawl(limit: Int = 1000)
  : (RDD[(String, String, String)], Long) = {
    val rdd = spark.sqlContext
      .read
      .format("jdbc")
      .options(dbConnOptions)
      .option("dbtable", "REPOS_QUEUE")
      .load()
      .select($"REPO_OWNER", $"REPOSITORY", $"BRANCH")
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
              |WHERE REPO_OWNER   = '%s'
              |  AND REPOSITORY   = '%s'
              |  AND BRANCH       = '%s';
            """.stripMargin.format(token, row.get(0), row.get(1), row.get(2))
        })
    )

    (rdd.map(x=> (x.get(0).toString, x.get(1).toString, x.get(2).toString)), token)
  }

  override def checkoutLinksToCrawl(limit: Int = 1000)
  : (RDD[(String, String, String, String)], Long) = {
    val rdd = spark.sqlContext
      .read
      .format("jdbc")
      .options(dbConnOptions)
      .option("dbtable", "FILE_HISTORY_QUEUE")
      .load()
      .select($"REPO_OWNER", $"REPOSITORY", $"BRANCH", $"GIT_PATH")
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
          |WHERE REPO_OWNER   = '%s'
          |  AND REPOSITORY   = '%s'
          |  AND BRANCH       = '%s'
          |  AND GIT_PATH     = '%s';
        """.stripMargin.format(token, row.get(0), row.get(1), row.get(2), row.get(3))
      })
    )

    (rdd.map(x=> (x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString)), token)
  }

  override def store(fht: RDD[FileHashTuple])
  : Unit = {
    execInBatch(fht
      .map(row => {
            """
              |INSERT IGNORE INTO FILE_HASH_HEAD(REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH, HASH_CODE)
              |VALUES ('%s', '%s', '%s', '%s', '%s');
            """.stripMargin.format(row.owner, row.repo, row.branch, row.gitPath, row.fileHash)
        })
    )
  }

  override def storeHistory(fht: RDD[FileHashTuple])
  : Unit = {
    execInBatch(fht
      .map(row => {
        """
          |INSERT IGNORE INTO FILE_HASH_HISTORY(REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH, HASH_CODE, COMMIT_ID, COMMIT_TIME)
          |VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d);
        """.stripMargin.format(row.owner, row.repo, row.branch, row.gitPath, row.fileHash, row.commitId, row.commitTime)
      }))
  }

  override def loadProjectsQueue(projects: RDD[String], flushExisting: Boolean)
  : Unit = {
    if (flushExisting) { // Truncate table
      val connection = getNewDBConnection
      connection
        .createStatement
        .execute("TRUNCATE TABLE REPOS_QUEUE;")
      connection.close()
    }

    execInBatch(projects
      .map(row => {
        val parts = row.split("/")
        if (parts.length != 2)   null
        else
          """INSERT IGNORE INTO REPOS_QUEUE(REPO_OWNER, REPOSITORY) VALUES ('%s', '%s');"""
            .stripMargin.format(parts(0), parts(1))
      }))
  }
}
