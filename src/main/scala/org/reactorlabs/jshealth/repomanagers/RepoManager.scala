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
    * @return a sequence of commits from that repo.
    */
  def getRepoFilesHistory(git: Git): Seq[FileHashTuple]

  /** Clones a given git repository.
    *
    * @param owner is the owner of the repo.
    * @param repo is the repo name.
    * @return a cloned git repo.
    */
  def gitCloneRepo(owner: String, repo: String): Git

  /** Given an git repo and objectId retrieves the contents of that object.
    *
    * @param rep is the git repo to fetch contents from.
    * @param objId is the hashcode/ object id of the object to retrieve contents.
    * @return A contents of that object.
    */
  def getFileContents(rep: Git, objId: String): Array[Byte]
}
