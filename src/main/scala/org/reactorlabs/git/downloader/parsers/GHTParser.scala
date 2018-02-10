package org.reactorlabs.git.downloader.parsers

import java.text.SimpleDateFormat

import org.reactorlabs.git.downloader.models.{GHTRecord, Languages}
import org.reactorlabs.git.downloader.models.Languages._

class GHTParser(languages:Set[Languages.Value] = Set(JavaScript, TypeScript, CoffeeScript)) {
  val df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")

  def parse(line: String): GHTRecord = {
    val record = line.split(",")
    try{
      val projUrl   = record(1).substring(29)
      val projLang  = Languages.withName(record(5))
      val isDeleted = record(8).toInt == 1
      val isForked  = record(7) != "\\N"
      val isCorrupt = false
      val created   = df.parse(record(6)).getTime

      GHTRecord(projUrl, projLang.id, isDeleted, isForked, isCorrupt, created)
    } catch {
      case _: Exception => GHTRecord("", -1, false, false, true, 0l)
    }
  }
}
