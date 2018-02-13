package org.reactorlabs.jshealth.git.datastores

import org.reactorlabs.jshealth.git.models.FileHashTuple

/** Stores data and metadata of a file transparently.
  * @author shabbir.ahussain
  */
trait DataStore {
  /** Stores the file hash tuple.
    *
    * @param fileHash is the input tuple.
    */
  def store(fileHash: FileHashTuple)

  /** Verifies if particular file is already downloaded.
    *
    * @param filHash is the hash of file to check.
    */
  def fileExists(filHash: String)

  def storeFile(filHash: String, data: String)
}
