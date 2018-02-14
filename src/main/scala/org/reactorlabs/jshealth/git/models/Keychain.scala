package org.reactorlabs.jshealth.git.models

import scala.collection.mutable

/** Manages keys to satisfy the rate limit.
  *
  * @param keyFilePath is the path of the text auth key file.
  *
  * @author shabbir.ahussain
  */
class Keychain(keyFilePath: String) {
  private val available = mutable.ListBuffer[String]()
  private val cooldownQ = mutable.PriorityQueue[(String, Long)]()(Ordering.by((x: (String, Long)) => -x._2))

  try{
    val source = scala.io.Source.fromFile(keyFilePath)
    source.getLines.map(x=> available += x )
    source.close()
    if (available.isEmpty) throw new Exception("No auth keys found at: " + keyFilePath)
  } catch {case e: Exception => throw e}

  private val rnd = scala.util.Random

  /**
    * @return Gets time in ms till a new key is available.
    */
  def getMinCooldownTime: Long = cooldownQ.head._2

  /** Gets the next available key from the keychain.
    *
    * @param key is the current key if used by client. Null is accepted if client is requesting key for the first time.
    * @param remaining is the X-RateLimit-Remaining for the key. (optional)
    * @param reset is the X-RateLimit-Reset for the key. (optional)
    * @return a key to use for next request.
    */
  def getNextKey(key: String, remaining: Int = 5000, reset: Long = 0): String = {
    if (remaining <= 0){
      available -= key
      cooldownQ += (key -> reset)
      return getNewKey
    }
    key
  }

  /**
    * @return a random key from the list of available keys.
    */
  private def getNewKey: String = {
    if (available.isEmpty){
      // Push keys from cooldown to available.
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


