package org.reactorlabs.jshealth.repomanagers

import org.eclipse.jgit.api.Git
import org.reactorlabs.jshealth.models.FileHashTuple

/**
  * @author shabbirahussain
  */
trait RepoManager {
  /** Given a git repo fetches file history from it.
    *
    * @param git is the git repo to fetch history from.
    * @return a sequence of commits from that repo and an optional contents field
    */
  def getRepoFilesHistory(git: Git): Seq[(FileHashTuple, Option[String])]

  /** Clones a given git repository.
    *
    * @param owner is the owner of the repo.
    * @param repo is the repo name.
    * @return a cloned git repo.
    */
  def gitCloneRepo(owner: String, repo: String): Git
}
