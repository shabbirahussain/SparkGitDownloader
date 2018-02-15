package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.reactorlabs.jshealth.models.FileHashTuple

/** Stores data and metadata of a file transparently.
  *
  * @author shabbir.ahussain
  */
trait DataStore extends Serializable{

  /** Verifies if particular file is already downloaded.
    *
    * @param filHash is the hash of file to check.
    */
//  def fileExists(filHash: String): Boolean
//  def storeFile(filHash: String, data: String): Unit

  /** Loads the project queue.
    *
    * @param projects is the RDD of project URLs.
    * @param flushExisting when set flushes the previous queue.
    */
  def loadProjectsQueue(projects: RDD[String], flushExisting: Boolean = false): Unit

  /** Marks the checkout repositories as completed.
    *
    * @param token is the access token to mark them complete.
    * @param errorRepo is the optional list of error repositories.
    */
  def markReposCompleted(token: String,
                         errorRepo: RDD[(String, String)] = null): Unit

  /**
    * @return an RDD of repos which are yet to be completed and a token required to mark them completed.
    */
  def checkoutReposToCrawl(limit: Int): (RDD[String], Long)

  /** Stores the file hash tuple.
    *
    * @param fileHash is the input tuple.
    */
  def store(fileHash: RDD[FileHashTuple]): Unit

  /** Stores the file hash tuple history.
    *
    * @param fileHash is the input tuple.
    */
  def storeHistory(fileHash: RDD[FileHashTuple]): Unit
}
