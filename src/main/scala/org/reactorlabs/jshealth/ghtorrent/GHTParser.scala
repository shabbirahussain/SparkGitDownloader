package org.reactorlabs.jshealth.ghtorrent

import java.text.SimpleDateFormat

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.reactorlabs.jshealth.models.{GHTRecord, Languages}

import scala.util.Try

/** This parser will only produce records that have valid language.
  * On any errors in parsing it will set error flag to true.
  *
  * @param languages is the set of Languages as defined in the emum.
  *
  * @author shabbir.ahussain
  */
@SerialVersionUID(100L)
class GHTParser(languages:Set[Languages.Value])
  extends Serializable {
  val df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")

  def parse(line: String): Try[GHTRecord] = {
    Try{
      val record = CSVParser
        .parse(line, CSVFormat.RFC4180)
        .iterator().next()

      val projUrl   = record.get(1).substring(29).split("/").map(_.trim)
      val projLang  = Languages.withName(record.get(5))
      val isDeleted = record.get(8).toInt == 1
      val isForked  = record.get(7) != "\\N"
      val created   = df.parse(record.get(6)).getTime

      GHTRecord(projUrl(0), projUrl(1), projLang.id, isDeleted, isForked, created)
    }
  }
}
