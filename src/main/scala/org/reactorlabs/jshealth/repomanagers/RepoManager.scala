package org.reactorlabs.jshealth.repomanagers

import org.reactorlabs.jshealth.models.FileHashTuple

/**
  * @author shabbirahussain
  */
trait RepoManager {
  /** Given an URL to explore lists all files available in the directory.
    *
    * @param url is the git url of format "<owner>/<repository>/<branch>:<path/to/dir>"
    * @return a sequence of directory entries from git API.
    */
  def listFiles(owner: String, repo:String, branch:String, gitPath: String): Seq[FileHashTuple]

  /** Given an URL fetches all the commits of that file.
    *
    * @param url is the git url of format "<owner>/<repository>/<branch>:<path/to/dir>"
    * @return a sequence of file commits entries from git API.
    */
  def getFileCommits(owner: String, repo:String, branch:String, gitPath: String): Seq[FileHashTuple]

  def getProject(owner: String, repo:String, branch:String, gitPath: String): Unit
}
