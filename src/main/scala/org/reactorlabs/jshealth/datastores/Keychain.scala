package org.reactorlabs.jshealth.datastores

import java.util.Date

import org.apache.log4j.Level

import scala.collection.mutable
import org.reactorlabs.jshealth.Main.logger

/** Manages keys to satisfy the rate limit.
  *
  * @param keyFilePath is the path of the text auth key file.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class Keychain(keyFilePath: String) extends Serializable {
  private val available = mutable.ListBuffer[String]()
  try{
    val source = scala.io.Source.fromFile(keyFilePath)
    source.getLines.foreach(x=>available += x.trim)
    source.close()
  } catch {case e: Exception => throw e}
  if (available.isEmpty) throw new RuntimeException("No auth keys found at: " + keyFilePath)

  private val rnd = scala.util.Random

  /** Gets the next available key from the keychain.
    * @return a key to use for next request.
    */
  def getNextKey(): String = available(rnd.nextInt(available.length - 1))
}


