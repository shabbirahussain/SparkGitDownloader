package org.reactorlabs.jshealth.repomanagers

import java.io.File

import org.reactorlabs.jshealth.models.FileHashTuple

/**
  * @author shabbirahussain
  */
trait RepoManager {

  /** Given an URL fetches all the commits of that file.
    *
    * @param owner is the owner of the git repo.
    * @param repo is the git repository.
    * @param branch is the branch of the git.
    * @return a sequence of file commits entries from git API.
    */
  def getFileCommits(owner: String, repo:String, branch:String): (Seq[FileHashTuple], File)
}
