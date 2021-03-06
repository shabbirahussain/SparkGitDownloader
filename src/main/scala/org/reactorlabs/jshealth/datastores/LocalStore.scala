package org.reactorlabs.jshealth.datastores

import java.sql.{Connection, DriverManager, ResultSet}

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
  implicit class ResultSetStream(resultSet: ResultSet) extends Iterator[ResultSet]{
//    def toStream: Stream[ResultSet] = {
//      new Iterator[ResultSet] {
//        def hasNext: Boolean =
//        def next(): ResultSet = resultSet
//      }.toStream
//    }
    override def hasNext: Boolean = resultSet.next()
    override def next(): ResultSet = resultSet
  }

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

  // ================ API Methods ===================

  override def markRepoCompleted(repo: RDD[(String, String)], token: Long)
  : Unit = {
    execInBatch(repo
      .map(row =>
        """
            |UPDATE REPOS_QUEUE
            |   SET COMPLETED   = TRUE,
            |       CHECKOUT_ID = %d
            |WHERE REPO_OWNER = '%s'
            |  AND REPOSITORY = '%s'
        """.stripMargin.format(token, escapeSql(row._1), escapeSql(row._2))
      ), autoCommit = true)
  }
  override def markRepoError(owner: String, repo: String, err: String)
  : Unit = {
    var errStr = escapeSql(err)
    errStr = errStr.substring(0, math.min(errStr.length, 254))
    execInBatch(Seq(
        """
          |UPDATE REPOS_QUEUE
          |   SET RESULT      = '%s',
          |       CHECKOUT_ID = null
          |WHERE REPO_OWNER = '%s'
          |  AND REPOSITORY = '%s'
          |  AND RESULT IS NULL
        """.stripMargin.format(errStr, escapeSql(owner), escapeSql(repo))
      ), autoCommit = true)
  }

  override def logRepoError(owner: String, repo: String, err: String): Unit = {
    // TODO: Implement
  }

  override def checkoutReposToCrawl(limit: Int = 1000)
  : (RDD[(String, String)], Long) = {
    val token = System.currentTimeMillis()
    val sql = """SELECT REPO_OWNER, REPOSITORY, CHECKOUT_ID
                |    FROM REPOS_QUEUE
                |    WHERE COMPLETED = 0
                |      AND RESULT IS NULL
                |      AND CHECKOUT_ID IS NULL
                |    LIMIT %d FOR UPDATE """
      .stripMargin.format(limit)

    val connection = getNewDBConnection
    connection.setAutoCommit(false)
    val statement  = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)

    val rows = statement.executeQuery(sql)
      .map(x=> {
        x.updateString("CHECKOUT_ID", token.toString)
        x.updateRow()
        (x.getString("REPO_OWNER"), x.getString("REPOSITORY"))
      })
      .toSeq
    rows.count(_=> true)
    val rdd = sc.parallelize(rows)

    connection.commit()
    connection.close()
    (rdd, token)
  }
  override def storeProjectsQueue(projects: RDD[(String, String)])
  : Unit = {
    // Setup the schema before loading
    execInBatch(Source
        .fromInputStream(this.getClass.getClassLoader.getResourceAsStream(mySqlSetupPathB4))
        .getLines()
        .mkString("\n")
        .split(";")
      , autoCommit = true)

    execInBatch(projects
      .map(row =>
        """INSERT IGNORE INTO REPOS_QUEUE(REPO_OWNER, REPOSITORY) VALUES ('%s', '%s');"""
            .stripMargin.format(row._1, escapeSql(row._2))
      ), autoCommit = true)

    // Setup the schema after loading
    execInBatch(Source
      .fromInputStream(this.getClass.getClassLoader.getResourceAsStream(mySqlSetupPathAfter))
      .getLines()
      .mkString("\n")
      .split(";"), autoCommit = true)

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
