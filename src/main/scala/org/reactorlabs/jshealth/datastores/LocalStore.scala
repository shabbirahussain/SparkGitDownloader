package org.reactorlabs.jshealth.datastores

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.models.Schemas
import org.apache.commons.lang.StringEscapeUtils.escapeSql
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress._
import org.apache.spark.sql.{DataFrame, SaveMode}
import scala.io.Source
import scala.util.{Failure, Try}

/**
  * @author shabbirahussain
  */
class LocalStore extends DataStore {
  private val tempPath = "_temporary"
  private val dataPath = "data"
  private val batchSize= 1000
  getNewDBConnection.close()

  import sqlContext.implicits._

  private def getNewDBConnection: Connection = {
    val url      = dbConnOptions.value("url")
    val driver   = dbConnOptions.value("driver")
    val schema   = dbConnOptions.value("schema")
    val username = dbConnOptions.value("username")
    val password = dbConnOptions.value("password")

    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, username, password)
    connection.setSchema(schema)
    connection
  }

  /** Executes batch of sql statements.
    *
    * @param sqls is the rdd of sql statements.
    * @param batchSize is the size of the batches to use.
    */
  private def execInBatch(sqls: RDD[String], autoCommit: Boolean, batchSize: Int = batchSize): Unit = {
    //sqls.foreach(println)
    sqls
      .mapPartitions(_.grouped(batchSize))
      .foreach(batch => execInBatch(batch, autoCommit))
  }
  /** Executes batch of sql statements.
    *
    * @param sqls is the rdd of sql statements.
    */
  private def execInBatch(sqls: Seq[String], autoCommit: Boolean): Unit = {
    //sqls.foreach(println)
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
  }

  /**
    * Sets up the database schema.
    */
  private def setupDatabase()
  : Unit = {
    val sql = Source
      .fromInputStream(this.getClass.getClassLoader.getResourceAsStream(mySqlSetupPath))
      .getLines()
      .mkString("\n")
      .split(";")

    execInBatch(sql, autoCommit = true)
  }
  // ================ API Methods ===================

  override def markRepoCompleted(repo: RDD[(String, String, String)], token: Long)
  : Unit = {
    execInBatch(repo
      .map(row =>
        """
            |UPDATE REPOS_QUEUE
            |   SET COMPLETED   = TRUE,
            |       CHECKOUT_ID = %d
            |WHERE REPO_OWNER = '%s'
            |  AND REPOSITORY = '%s'
            |  AND BRANCH     = '%s'
        """.stripMargin.format(token, escapeSql(row._1), escapeSql(row._2), row._3)
      ), autoCommit = true)
  }
  override def markRepoError(owner: String, repo: String, branch: String, err: String, token: Long)
  : Unit = {
    execInBatch(Seq(
        """
          |UPDATE REPOS_QUEUE
          |   SET RESULT      = '%s',
          |       CHECKOUT_ID = %d
          |WHERE REPO_OWNER = '%s'
          |  AND REPOSITORY = '%s'
          |  AND BRANCH     = '%s'
        """.stripMargin.format(escapeSql(err), token, escapeSql(owner), escapeSql(repo), branch)
      ), autoCommit = true)
  }
  override def checkoutReposToCrawl(limit: Int = 1000)
  : (RDD[(String, String, String)], Long) = {
    val rdd = sqlContext
      .read
      .format("jdbc")
      .options(dbConnOptions.value)
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
        }), batchSize = limit, autoCommit = true
    )
    (rdd, token)
  }
  override def storeProjectsQueue(projects: RDD[(String, String)])
  : Unit = {
    setupDatabase() // Drop and create new queue.

    execInBatch(projects
      .map(row =>
        """INSERT IGNORE INTO REPOS_QUEUE(REPO_OWNER, REPOSITORY) VALUES ('%s', '%s');"""
            .stripMargin.format(row._1, escapeSql(row._2))
      ), autoCommit = true)
  }

  override def getExistingHashes()
  : Seq[String] = {
    Try(read(Schemas.INDEX).map(_.getAs[String](0)).collect)
    match {
      case _ @Failure(_) => Seq.empty
      case success @ _   => success.get
    }
  }

  override def read(split: Schemas.Value): DataFrame = {
    val meta = Schemas.asMap(key = split)
    sqlContext.read
      .schema(meta._1)
      .load(fileStorePath + "/%s/*/%s/".format(dataPath, split))
      .dropDuplicates(meta._2)
  }

  override def storeRecords(records: DataFrame, folder: String)
  : Unit = {
    records
      .write
      .option("codec", classOf[BZip2Codec].getName)
      .mode(SaveMode.Overwrite)
      .save("%s/%s/%s".format(fileStorePath, dataPath, folder))
  }

  override def consolidateData(fileTypes: Set[Schemas.Value]): Unit = {
    val finalDestination = System.currentTimeMillis().toString
    fs.delete(new Path("%s/%s".format(fileStorePath, tempPath)), true)

    fileTypes.foreach(x=> {
      storeRecords(read(x), folder = finalDestination)
    })
    fs.delete(new Path("%s/%s".format(fileStorePath, dataPath)), true)
    fs.mkdirs(new Path("%s/%s".format(fileStorePath, dataPath)))
    fileTypes.foreach(x=> {
      val oldPath = new Path("%s/%s/%s/%s".format(fileStorePath, tempPath, x, x))
      val newPath = new Path("%s/%s/%s/%s".format(fileStorePath, dataPath, finalDestination, x))
      fs.rename(oldPath, newPath)
    })
    fs.delete(new Path("%s/%s".format(fileStorePath, tempPath)), true)
    fs.create(new Path("%s/%s/%s/_SUCCESS".format(fileStorePath, dataPath, finalDestination))).close()
  }
}
