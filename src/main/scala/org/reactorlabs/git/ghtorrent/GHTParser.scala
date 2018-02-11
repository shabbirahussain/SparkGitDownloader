package org.reactorlabs.git.ghtorrent

import java.text.SimpleDateFormat

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.reactorlabs.git.ghtorrent.models.Languages
import org.reactorlabs.git.ghtorrent.models.Languages._

/** This parser will only produce records that have valid language.
  * On any errors in parsing it will set error flag to true.
  *
  * @param languages is the set of Languages as defined in the emum.
  *
  * @author shabbir.ahussain
  */
@SerialVersionUID(100L)
class GHTParser(languages:Set[Languages.Value] = Set(JavaScript, TypeScript, CoffeeScript))
  extends Serializable {
  val df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")

  def parse(line: String): (String, (Int, Boolean, Boolean, Boolean, Long)) = {
    try{
      val record = CSVParser
        .parse(line, CSVFormat.RFC4180)
        .iterator().next()

      val projUrl   = record.get(1).substring(29)
      val projLang  = Languages.withName(record.get(5))
      val isDeleted = record.get(8).toInt == 1
      val isForked  = record.get(7) != "\\N"
      val isCorrupt = false
      val created   = df.parse(record.get(6)).getTime

      (projUrl, (projLang.id, isDeleted, isForked, isCorrupt, created))
    } catch {
      case e: Exception => (e.toString, (-1, false, false, true, 0l))
    }
  }
}
