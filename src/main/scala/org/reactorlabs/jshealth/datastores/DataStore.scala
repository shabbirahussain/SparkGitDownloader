package org.reactorlabs.jshealth.datastores

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.reactorlabs.jshealth.models.{FileHashTuple, Schemas}

/** Stores data and metadata of a file transparently.
  *
  * @author shabbirahussain
  */
trait DataStore extends Serializable{
  /** Loads the project queue.
    *
    * @param projects is the RDD of project URLs.
    * @param flushExisting when set flushes the previous queue.
    */
  def loadProjectsQueue(projects: RDD[String], flushExisting: Boolean = false): Unit

  /** Marks the checkout repositories as completed.
    *
    * @param repo is the repo to update in format (onwer, repository, branch)
    */
  def markRepoCompleted(repo: RDD[(String, String, String)]): Unit

  /**
    * @param limit is the maximum number of records to fetch.
    * @return an RDD of repos which are yet to be completed and a token required to mark them completed.
    */
  def checkoutReposToCrawl(limit: Int): (RDD[(String, String, String)], Long)

  /** Stores the file hash tuple history.
    *
    * @param record is the input Dataframe of (SPLIT, TRUE_KEY, VALUE). Here key is defined by partition and a natural key.
    * @param folder is the output folder to save to.
    */
  def store(record: DataFrame, folder: String): Unit

  /** Marks a repo with error.
    *
    * @param owner is the owner of the repo.
    * @param repo is the repo name.
    * @param branch is the branch of repo.
    * @param err is the error message to update.
    */
  def markRepoError(owner: String, repo: String, branch: String, err: String): Unit

  /**
    * @return a seq of hashes whos content is already downloaded.
    */
  def getExistingHashes(): Seq[String]

  /**
    * @return an unique and unified view of given split.
    */
  def read(split: Schemas.Value): DataFrame

  /**
    * Consolidates data of multiple runs into one.
    */
  def consolidateData(): Unit
}
