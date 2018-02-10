package org.reactorlabs.git.downloader.parsers

import java.text.SimpleDateFormat

import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.reactorlabs.git.downloader.models.{GHTRecord, Languages}
import org.reactorlabs.git.downloader.models.Languages._

@SerialVersionUID(100L)
class GHTParser(languages:Set[Languages.Value] = Set(JavaScript, TypeScript, CoffeeScript))
  extends Serializable {
  val df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")

  def parse(line: String): GHTRecord = {
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
//      GHTRecord("", -1, false, false, true, 0l)
      GHTRecord(projUrl, projLang.id, isDeleted, isForked, isCorrupt, created)
    } catch {
      case e: Exception => GHTRecord(e.toString, -1, false, false, true, 0l)
    }
  }
}
