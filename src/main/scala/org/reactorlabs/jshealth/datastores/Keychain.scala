package org.reactorlabs.jshealth.datastores

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
  private val cooldownQ = mutable.PriorityQueue[(String, Long)]()(Ordering.by((x: (String, Long)) => -x._2))

  try{
    val source = scala.io.Source.fromFile(keyFilePath)
    source.getLines.foreach(x=>available += x)
    source.close()
    if (available.isEmpty) throw new Exception("No auth keys found at: " + keyFilePath)
  } catch {case e: Exception => throw e}

  private val rnd = scala.util.Random

  /**
    * @return Gets time in ms till a new key is available.
    */
  def getMinCooldownTime: Long = {
    if (available.nonEmpty) return 0
    if (cooldownQ.isEmpty)  {
      val msg = "No usable git api keys found."
      logger.log(Level.FATAL, msg)
      throw new Exception(msg)
    }
    cooldownQ.head._2
  }

  /** Gets the next available key from the keychain.
    *
    * @param key is the current key if used by client. Null is accepted if client is requesting key for the first time.
    * @param remaining is the X-RateLimit-Remaining for the key.
    * @param reset is the X-RateLimit-Reset for the key.
    * @param isValid indicates if previously used key was a valid key or not.
    * @return a key to use for next request.
    */
  def getNextKey(key: String, remaining: Int, reset: Long, isValid: Boolean): String = {
    if (!isValid) {
      available -= key
    } else {
      if (remaining > 0) return key

      available -= key
      cooldownQ += (key -> reset)
    }
    getNewKey
  }

  /**
    * @return a random key from the list of available keys.
    */
  private def getNewKey: String = {
    if (available.isEmpty){
      // Push keys from cooldown to available list.
      while(cooldownQ.nonEmpty && cooldownQ.head._2 < System.currentTimeMillis) {
        available += cooldownQ.dequeue._1
      }
    }

    // Return a random key from available keys.
    if (available.nonEmpty){
      return available(rnd.nextInt(available.length))
    }
    null
  }
}


