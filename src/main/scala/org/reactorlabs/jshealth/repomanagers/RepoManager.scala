package org.reactorlabs.jshealth.repomanagers

import org.reactorlabs.jshealth.models.FileHashTuple

trait RepoManager {
  /** Given an URL to explore lists all files available in the directory.
    *
    * @param url is the git url of format "<owner>/<repository>/<branch>:<path/to/dir>"
    * @return a sequence of directory entries from git API.
    */
  def listFiles(url: String): Seq[FileHashTuple]

  /** Given an URL fetches all the commits of that file.
    *
    * @param url is the git url of format "<owner>/<repository>/<branch>:<path/to/dir>"
    * @return a sequence of file commits entries from git API.
    */
  def getFileCommits(url: String): Seq[FileHashTuple]

  def getProject(url: String): Unit
}
