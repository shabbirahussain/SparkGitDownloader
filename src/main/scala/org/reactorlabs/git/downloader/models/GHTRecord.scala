package org.reactorlabs.git.downloader.models

final case class GHTRecord(projUrl  : String,
                     projLang : Int,
                     isDeleted: Boolean,
                     isForked : Boolean,
                     isCorrupt: Boolean,
                     created  : Long)