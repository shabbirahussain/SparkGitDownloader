package org.reactorlabs.jshealth.models

/**
  * @param projOwner is the project URL as string.
  * @param projRepo is the project URL as string.
  * @param projLang is the project language as integer.
  * @param isDeleted represents if project is in deleted state.
  * @param isForked represents if thta project is forked.
  * @param created is the time represents of the created date.
  *
  * @author shabbirahussain
  */
final case class GHTRecord(projOwner: String,
                           projRepo : String,
                           projLang : Int,
                           isDeleted: Boolean,
                           isForked : Boolean,
                           created  : Long)