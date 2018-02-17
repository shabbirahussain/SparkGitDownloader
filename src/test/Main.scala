

import java.text.SimpleDateFormat
import java.util.Date

import Main.timeBlock
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main {
  val df = new SimpleDateFormat("yyyy/mm/dd HH:mm:ss")
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Task A6")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val songs = loadSongCSV(sc, "input/song_info.csv.gz")
      .map(_.DURATION).cache()
    songs.count()
    songs
      .map {x=>Math.pow(x, 1)}
      .map {x=>Math.pow(x, 2)}
      .map {x=>Math.pow(x, 3)}
      .map {x=>Math.pow(x, 4)}
      .map {x=>Math.pow(x, 5)}
      .map {x=>Math.pow(x, 6)}
      .map {x=>Math.pow(x, 7)}
      .map {x=>Math.pow(x, 8)}
      .count







    timeBlock{songs
      .map {x=>
        Math.pow(
          Math.pow(
            Math.pow(
              Math.pow(
                Math.pow(
                  Math.pow(
                    Math.pow(
                      Math.pow(x, 1), 2), 3), 4), 5), 6), 7), 8)}
      .count}
    timeBlock{songs
      .map {x=>Math.pow(x, 1)}
      .map {x=>Math.pow(x, 2)}
      .map {x=>Math.pow(x, 3)}
      .map {x=>Math.pow(x, 4)}
      .map {x=>Math.pow(x, 5)}
      .map {x=>Math.pow(x, 6)}
      .map {x=>Math.pow(x, 7)}
      .map {x=>Math.pow(x, 8)}
      .count}


    println("\n\n")
    timeBlock{songs
      .count()
    }
    timeBlock{songs
      .map {x=>Math.pow(x, 1)}
      .count()
    }
    timeBlock{songs
      .map {x=>Math.pow(x, 1)}
      .map {x=>Math.pow(x, 1)}
      .count()
    }
    spark.stop()
  }


  def timeBlock[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    println("-- Start time:" + df.format(new Date(t0)))
    val res = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("-- Elapsed time: " + (t1 - t0) + "ms")
    res
  }

  def loadSongCSV(sc: SparkContext, path : String):RDD[SongRecord] ={
    sc.textFile(path)
      .mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map[SongRecord] {new SongRecord(_)}
  }
}

class SongRecord(line: String) extends Serializable {
  private val tokens = line.split(";")
  def toSafeDouble(v : String, default : Double): Double = try{v.toDouble} catch{ case _:Exception => default}
  def toSafeInt(v : String, default : Int): Int = try{v.toDouble.toInt} catch{ case _:Exception => default}

  var TRACK_ID : String                   = tokens(0)
  var AUDIO_MD5 : String                  = tokens(1)
  var END_OF_FADE_IN : String             = tokens(2)
  var START_OF_FADE_OUT : String          = tokens(3)
  var ANALYSIS_SAMPLE_RATE : String       = tokens(4)
  var DURATION : Double                   = toSafeDouble(tokens(5), 0)
  var LOUDNESS : Double                   = toSafeDouble(tokens(6), 0)
  var TEMPO : Double                      = toSafeDouble(tokens(7), 0)
  var KEY : String                        = tokens(8)
  var KEY_CONFIDENCE : Double             = toSafeDouble(tokens(9), 0)
  var MODE : String                       = tokens(10)
  var MODE_CONFIDENCE : String            = tokens(11)
  var TIME_SIGNATURE : String             = tokens(12)
  var TIME_SIGNATURE_CONFIDENCE : String  = tokens(13)
  var DANCEABILITY : String               = tokens(14)
  var ENERGY : String                     = tokens(15)
  var ARTIST_ID : String                  = tokens(16)
  var ARTIST_NAME : String                = tokens(17)
  var ARTIST_LOCATION : String            = tokens(18)
  var ARTIST_FAMILIARITY : Double         = toSafeDouble(tokens(19), 0)
  var ARTIST_HOTTTNESSS : Double          = toSafeDouble(tokens(20), 0)
  var GENRE : String                      = tokens(21)
  var RELEASE : String                    = tokens(22)
  var SONG_ID : String                    = tokens(23)
  var TITLE : String                      = tokens(24)
  var SONG_HOTTTNESSS : Double            = toSafeDouble(tokens(25), 0)
  var YEAR : Int                          = toSafeInt(tokens(26), 0)
}
