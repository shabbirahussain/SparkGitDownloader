package org.reactorlabs.git.models

/**
  * @param projUrl is the project URL as string.
  * @param projLang is the project language as integer.
  * @param isDeleted represents if project is in deleted state.
  * @param isForked represents if thta project is forked.
  * @param isCorrupt represents any error in parsing of the record.
  * @param created is the time represents of the created date.
  *
  * @author shabbir.ahussain
  */
final case class GHTRecord(projUrl  : String,
                     projLang : Int,
                     isDeleted: Boolean,
                     isForked : Boolean,
                     isCorrupt: Boolean,
                     created  : Long)