package org.reactorlabs.jshealth

import java.net.URLClassLoader
import java.util.{Date, Properties}

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.reactorlabs.jshealth.datastores.{DataStore, LocalStore}

import scala.io.Source

/**
  * @author shabbirahussain
  */
object Main extends Serializable {
  val logger:Logger = Logger.getLogger("project.defaultLogger")

  val prop: Properties = new Properties()
  try {
    val loader = this.getClass.getClassLoader
    val stream = loader.getResourceAsStream("resources/conf/config-defaults.properties")
    prop.load(stream)
    stream.close()

//    val stream1 = new FileInputStream("config.properties")
//    prop.load(stream1)
//    stream1.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

  val spark: SparkSession = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .master("local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setCheckpointDir("target/temp/spark/")

  val sqlContext: SQLContext = spark.sqlContext

  val hconf = sc.hadoopConfiguration
  hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  hconf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  val fs: FileSystem = FileSystem.get(hconf)
  val dbConnOptions: Broadcast[Map[String, String]] = {
    val driver    = prop.getProperty("ds.mysql.driver")
    val url       = prop.getProperty("ds.mysql.url")
    val username  = prop.getProperty("ds.mysql.username")
    val schema    = prop.getProperty("ds.mysql.schema")

    val source    = Source.fromFile(prop.getProperty("ds.mysql.password.path"))
    val password  = source.mkString
    source.close()

    sc.broadcast(
      Map("driver"  -> driver,
        "url"       -> url,
        "username"  -> username,
        "user"      -> username,
        "password"  -> password,
        "schema"    -> schema))
  }
  val mySqlSetupPath: String = prop.getProperty("ds.mysql.setup.path")
  val fileStorePath: String  = prop.getProperty("ds.file.store.path")
  val ds: DataStore = new LocalStore()

  def main(args: Array[String])
  : Unit = {
    var module: String = ""
    if(args.length == 0) {
      print("Please enter (ght/git) to select a module to execute:")
      module = scala.io.StdIn.readLine()
    } else {
      module = args(0)
    }

    var start = 0l
    println("started at:" + new Date())
    start = System.currentTimeMillis()

    module match {
      case "ght" => ghtorrent.Main.main(Array[String]())
      case "git" => git.Main.main(Array[String]())
      case _=> throw new Exception("Unsupported argument. Check readme for more info.")
    }
    println("\nended at:" + new Date() + "\ttook:"+ (System.currentTimeMillis() - start))
  }
}
