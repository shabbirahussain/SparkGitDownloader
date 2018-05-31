package org.reactorlabs.jshealth

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}
import java.net.URLClassLoader
import java.util.{Date, Properties}

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.reactorlabs.jshealth.datastores.{DataStore, LocalStore}

import scala.io.Source

/**
  * @author shabbirahussain
  */
object Main extends Serializable {
  val logger:Logger = Logger.getLogger("project.defaultLogger")


  val spark: SparkSession = SparkSession
    .builder()
    .appName("ReactorLabs Git Miner")
    .master("local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val sqlContext: SQLContext = spark.sqlContext

  val prop: Properties = new Properties()
  try {
//        ClassLoader
//          .getSystemClassLoader
//          .asInstanceOf[URLClassLoader]
//          .getURLs
//          .foreach(x=> println(x.getFile))

    val path = SparkFiles.get("config-defaults.properties")
    val stream = new FileInputStream(path) //this.getClass.getClassLoader.getResourceAsStream(path)
    prop.load(stream)

    stream.close()
  } catch { case e: Exception => e.printStackTrace(); sys.exit(1)}

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
        "schema"    -> schema,
        "isolationLevel" -> "READ_COMMITTED"
      ))
  }
  val mySqlSetupPathB4: String = prop.getProperty("ds.mysql.before.setup.path")
  val mySqlSetupPathAfter: String = prop.getProperty("ds.mysql.after.setup.path")
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
