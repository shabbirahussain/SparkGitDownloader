package org.reactorlabs.jshealth.datastores

import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.models.{FileHashTuple, Schemas}
import org.apache.commons.lang.StringEscapeUtils.escapeSql
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.{Failure, Try}

/**
  * @author shabbirahussain
  */
class LocalStore(batchSize: Int, fileStorePath: String, fs: FileSystem) extends DataStore {
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

  private def store(records: DataFrame,
                    folder1: String,
                    folder2: String,
                    quote: String,
                    delimiter: String)
  : Unit = {
    records
      .coalesce(1)
      .write
      .partitionBy("SPLIT")
      .option("codec", classOf[BZip2Codec].getName)
      .option("quote", quote)
      .option("delimiter", delimiter)
      .format("com.databricks.spark.csv")
      .save("%s/%s/%s".format(fileStorePath, folder1,folder2))
  }

  // ================ API Methods ===================

  override def markRepoCompleted(repo: RDD[(String, String, String)])
  : Unit = {
    execInBatch(repo
      .map(row =>
        """
            |UPDATE REPOS_QUEUE
            |   SET COMPLETED   = TRUE
            |WHERE REPO_OWNER = '%s'
            |  AND REPOSITORY = '%s'
            |  AND BRANCH     = '%s'
        """.stripMargin.format(escapeSql(row._1), escapeSql(row._2), row._3)
      ), autoCommit = true)
  }

  override def markRepoError(owner: String, repo: String, branch: String, err: String): Unit = {
    execInBatch(Seq(
        """
          |UPDATE REPOS_QUEUE
          |   SET RESULT      = '%s'
          |WHERE REPO_OWNER = '%s'
          |  AND REPOSITORY = '%s'
          |  AND BRANCH     = '%s'
        """.stripMargin.format(escapeSql(err), escapeSql(owner), escapeSql(repo), branch)
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
      .filter($"CHECKOUT_ID".isNull)
      .limit(limit)
      .rdd
      .map(x=> (x.get(0).toString, x.get(1).toString, x.get(2).toString))

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
            """.stripMargin.format(token, escapeSql(row._1), escapeSql(row._2), row._3)
        }), batchSize = limit, autoCommit = false
    )
    (rdd, token)
  }

  override def storeHistory(records: DataFrame, folder: String)
  : Unit = store(records, "data", folder, "\u0000", "\t")


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
            .stripMargin.format(parts(0).toLowerCase(), escapeSql(parts(1)).toLowerCase())
      }), autoCommit = true)
  }

  override def getExistingHashes()
  : Seq[String] = {
    Try(read("indexes").map(_.getAs[String](0)).collect)
    match {
      case _ @Failure(e) => Seq.empty
      case success @ _   => success.get
    }
  }

  // Readers
  override def read(split: String): DataFrame = {
    val meta = Schemas.asMap(key = split)
    sqlContext.read
      .schema(meta._1)
      .option("quote", "\"")
      .csv(fileStorePath + "/data/*/SPLIT=%s/".format(split))
      .dropDuplicates(meta._2)
  }

  override def consolidateData(): Unit = {
    val finalDestination = System.currentTimeMillis().toString
    fs.delete(new Path("%s/%s".format(fileStorePath, "temp")), true)

    Schemas.asMap.foreach(x=> {
      store(read(x._1).withColumn("SPLIT", lit(x._1)), "temp", x._1, "\"", ",")
    })
    fs.delete(new Path("%s/data".format(fileStorePath)), true)
    fs.mkdirs(new Path("%s/data".format(fileStorePath)))
    Schemas.asMap.foreach(x=> {
      val oldPath = new Path("%s/%s/%s/SPLIT=%s".format(fileStorePath, "temp", x._1, x._1))
      val newPath = new Path("%s/%s/%s/SPLIT=%s".format(fileStorePath, "data", finalDestination, x._1))
      fs.rename(oldPath, newPath)
    })
    fs.delete(new Path("%s/%s".format(fileStorePath, "temp")), true)
    fs.create(new Path("%s/%s/%s/_SUCCESS".format(fileStorePath, "data", finalDestination))).close()
  }
}
