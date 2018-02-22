package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.models.FileHashTuple
import org.apache.commons.lang.StringEscapeUtils.escapeSql
import org.apache.log4j.Level

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
  private def execInBatch(sqls: RDD[String], autoCommit: Boolean, batchSize: Int = batchSize): Unit = {
    //sqls.foreach(println)
    sqls
      .mapPartitions(_.grouped(batchSize))
      .foreach(batch => {
        val connection = getNewDBConnection
        val statement  = connection.createStatement()
        connection.setAutoCommit(autoCommit)

        batch
          .filter(_ != null)
          .foreach(sql => statement.addBatch(sql))
        statement.executeBatch()

        if (!autoCommit) connection.commit()
        connection.close()
      })
  }
  /** Executes batch of sql statements.
    *
    * @param sqls is the rdd of sql statements.
    */
  private def execInBatch(sqls: Seq[String], autoCommit: Boolean): Unit = {
    //sqls.foreach(println)
    val connection = getNewDBConnection
    val statement  = connection.createStatement()

    sqls.grouped(batchSize)
      .foreach(batch=> {
        val connection = getNewDBConnection
        val statement  = connection.createStatement()
        connection.setAutoCommit(autoCommit)

        batch
          .filter(_ != null)
          .foreach(sql => statement.addBatch(sql))
        statement.executeBatch()

        if (!autoCommit) connection.commit()
        connection.close()
      })

    connection.close()
  }

  // ================ API Methods ===================

  override def markRepoCompleted(fht: FileHashTuple)
  : Unit = {
    val sql = """
                |UPDATE REPOS_QUEUE
                |   SET COMPLETED   = TRUE,
                |       CHECKOUT_ID = NULL
                |WHERE REPO_OWNER = '%s'
                |  AND REPOSITORY = '%s'
              """.stripMargin.format(fht.owner, escapeSql(fht.repo))
    val sqlE= """
                |UPDATE REPOS_QUEUE
                |   SET COMPLETED   = TRUE,
                |       CHECKOUT_ID = NULL,
                |       RESULT      = '%s'
                |WHERE REPO_OWNER = '%s'
                |  AND REPOSITORY = '%s'
              """.stripMargin.format(escapeSql(fht.error), fht.owner, escapeSql(fht.repo))

    execInBatch(Seq(if(fht.error == null) sql else sqlE), autoCommit = true)
  }

  override def checkoutReposToCrawl(limit: Int = 1000)
  : (RDD[(String, String, String)], Long) = {
    val rdd = sqlContext
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
            """.stripMargin.format(token, row.get(0), escapeSql(row.get(1).toString), row.get(2))
        }), batchSize = limit, autoCommit = false
    )

    (rdd.map(x=> (x.get(0).toString, x.get(1).toString, x.get(2).toString)), token)
  }

  override def storeHistory(fht: Seq[FileHashTuple])
  : Unit = {
    execInBatch(fht
      .map(row => {
        """
          |INSERT IGNORE INTO FILE_HASH_HISTORY(REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH, HASH_CODE, COMMIT_ID, COMMIT_TIME, BYTE_SIZE, MESSAGE, IS_BUG_FIX)
          |VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %s);
        """.stripMargin.format(row.owner, escapeSql(row.repo), row.branch, escapeSql(row.gitPath), row.fileHash, row.commitId, row.commitTime, row.byteSize, escapeSql(row.commitMsg).replaceAll("[\n|'|\\\\]"," "), row.isBugFix)
      }), autoCommit = true)
  }

  override def loadProjectsQueue(projects: RDD[String], flushExisting: Boolean)
  : Unit = {
    if (flushExisting) { // Truncate table
      val sql = "TRUNCATE TABLE REPOS_QUEUE;"
      execInBatch(Seq(sql), autoCommit = true)
    }

    execInBatch(projects
      .map(row => {
        val parts = row.split("/")
        if (parts.length != 2)   null
        else
          """INSERT IGNORE INTO REPOS_QUEUE(REPO_OWNER, REPOSITORY) VALUES ('%s', '%s');"""
            .stripMargin.format(parts(0), escapeSql(parts(1)))
      }), autoCommit = true)
  }
}
