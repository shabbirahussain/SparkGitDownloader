package org.reactorlabs.jshealth

import java.io.FileInputStream
import java.util.{Date, Properties}
import java.sql.DriverManager
import java.sql.Connection

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.reactorlabs.jshealth.datastores.{DataStore, LocalStore}

import scala.collection.mutable
import scala.io.Source

/**
  * @author shabbirahussain
  */
object Main extends Serializable {
  val logger:Logger = Logger.getLogger("project.default.logger")

  val prop: Properties = new Properties()
  try {
    val loader = this.getClass.getClassLoader
    val stream = loader.getResourceAsStream("config-defaults.properties")
    prop.load(stream)
    stream.close()

//    val stream1 = new FileInputStream("config.properties")
//    prop.load(stream1)
//    stream1.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  var dbConnOptions: mutable.Map[String, String] = mutable.Map[String, String]()
  try{
    val driver    = prop.getProperty("ds.mysql.driver")
    val url       = prop.getProperty("ds.mysql.url")
    val username  = prop.getProperty("ds.mysql.username")
    val schema    = prop.getProperty("ds.mysql.schema")

    val source    = Source.fromFile(prop.getProperty("ds.mysql.password.path"))
    val password  = source.mkString
    source.close()

    dbConnOptions += ("driver"    -> driver)
    dbConnOptions += ("url"       -> url)
    dbConnOptions += ("username"  -> username)
    dbConnOptions += ("user"      -> username)
    dbConnOptions += ("password"  -> password)
    dbConnOptions += ("schema"    -> schema)
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val spark: SparkSession = SparkSession
    .builder()
    .master(prop.getProperty("spark.master"))
    .appName("ReactorLabs Git Miner")
    .config("spark.cores.max",       prop.getProperty("spark.cores.max"))
    .config("spark.executor.cores",  prop.getProperty("spark.executor.cores"))
    .config("spark.workers.cores",   prop.getProperty("spark.executor.cores"))
    .config("spark.executor.memory", prop.getProperty("spark.executor.memory"))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  sc.setCheckpointDir("target/temp/spark/")

  val sqlContext: SQLContext = spark.sqlContext

  val dataStore: DataStore = new LocalStore(prop.getProperty("ds.mysql.batch.size").toInt)

  def main(args: Array[String])
  : Unit = {
    println("Main")
    var start = 0l
    println("started at:" + new Date())
    start = System.currentTimeMillis()


//    ghtorrent.Main.main(Array[String]())
    git.Main.main(Array[String]())

//    sc.textFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects.csv")
//      .saveAsTextFile("/Users/shabbirhussain/Data/project/mysql-2018-02-01/projects_full.csv")

    println("\nended at:" + new Date() + "\ttook:"+ (System.currentTimeMillis() - start))
  }

  def getNewDBConnection: Connection = {
    val url      = dbConnOptions.get("url").get
    val driver   = dbConnOptions.get("driver").get
    val schema   = dbConnOptions.get("schema").get
    val username = dbConnOptions.get("username").get
    val password = dbConnOptions.get("password").get

    Class.forName(driver)
    val connection: Connection = DriverManager.getConnection(url, username, password)
    connection.setSchema(schema)
    connection
  }
}
