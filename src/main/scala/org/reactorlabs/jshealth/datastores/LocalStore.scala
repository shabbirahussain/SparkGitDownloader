package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.models.FileHashTuple
import org.apache.commons.lang.StringEscapeUtils.escapeSql
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.log4j.Level

/**
  * @author shabbirahussain
  */
class LocalStore(batchSize: Int, fileStorePath: String) extends DataStore {
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

  override def markRepoCompleted(repo: RDD[(String, String, String)])
  : Unit = {
    execInBatch(repo
      .map(row =>
        """
            |UPDATE REPOS_QUEUE
            |   SET COMPLETED   = TRUE,
            |       CHECKOUT_ID = NULL
            |WHERE REPO_OWNER = '%s'
            |  AND REPOSITORY = '%s'
            |  AND BRANCH     = '%s'
        """.stripMargin.format(row._1, escapeSql(row._2), row._3)
      ), autoCommit = true)
  }

  override def markRepoError(owner: String, repo: String, branch: String, err: String): Unit = {
    execInBatch(Seq(
        """
          |UPDATE REPOS_QUEUE
          |   SET COMPLETED   = TRUE,
          |       CHECKOUT_ID = NULL,
          |       RESULT      = '%s'
          |WHERE REPO_OWNER = '%s'
          |  AND REPOSITORY = '%s'
          |  AND BRANCH     = '%s'
        """.stripMargin.format(owner, escapeSql(repo), branch, escapeSql(err))
      ), autoCommit = true)
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

  override def storeHistory(fht: RDD[FileHashTuple])
  : Unit = {
    fht
      .map(row => {
        """"%s","%s","%s","%s",%d""".stripMargin
          .format(row.owner, row.repo, row.gitPath, row.fileHash, row.commitTime)
          .replaceAll(""""null"""", "")
      })
      .coalesce(1, shuffle = true)
      .saveAsTextFile(fileStorePath + System.currentTimeMillis(), classOf[BZip2Codec])
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
