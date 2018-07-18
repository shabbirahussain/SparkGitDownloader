package org.reactorlabs.jshealth.repomanagers

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevCommit
import org.reactorlabs.jshealth.models.{CommitMessageRecord, FileMetadataRecord}

import scala.util.Try

/**
  * @author shabbirahussain
  */
trait RepoManager extends Serializable {
  /** Clones a given git repository.
    *
    * @param owner is the owner of the repo.
    * @param repo is the repo name.
    * @return a cloned git repo.
    */
  def cloneRepo(owner: String, repo: String)
  : Iterator[Try[Git]]

  /**
    *  Given a cloned git repository returns commits in it in asc order.
    */
  def getAllRevCommits(git: Git)
  : Try[Iterator[RevCommit]]

  /**
    *  Given a cloned git repository returns commits in it in asc order.
    */
  def getAllCommits(git: Git)
  : Try[Iterator[CommitMessageRecord]]


  /** Given a git repo fetches file history from it.
    *
    * @return a sequence of commits from that repo.
    */
  def getRepoFilesHistory(git:Git, commits: Iterator[RevCommit])
  : Iterator[Try[FileMetadataRecord]]

  /** Given an git repo and objectId retrieves the contents of that object.
    *
    * @param git is the git repo to fetch contents from.
    * @param objIds is the hashcode/ object id of the object to retrieve contents.
    * @return A contents of that object.
    */
  def getFileContents(git: Git, objIds: Iterator[String])
  : Iterator[Try[Array[Byte]]]
}

object RepoManager {
  /**
    * @param git is the cloned git repo object to fetch details from.
    * @return Owner of the repo and repository name.
    */
  def getRepoOwnership(git: Git): (String, String) = {
    val dir = git.getRepository.getDirectory.getParentFile
    (dir.getParentFile.getName, dir.getName)
  }

  case class AutoCleanIterator()
}