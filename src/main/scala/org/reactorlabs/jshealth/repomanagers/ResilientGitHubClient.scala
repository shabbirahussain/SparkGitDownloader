package org.reactorlabs.jshealth.repomanagers

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.{CheckoutConflictException, InvalidRemoteException, JGitInternalException, NoHeadException}
import org.eclipse.jgit.errors.MissingObjectException
import org.eclipse.jgit.revwalk.RevCommit
import org.reactorlabs.jshealth.datastores.DataStore
import org.reactorlabs.jshealth.models.{CommitMessageRecord, FileMetadataRecord}
import org.reactorlabs.jshealth.util.TimedIterator

import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

/**
  * @author shabbirahussain
  * @param rman is the base repository manager to use.
  * @param ds is the data store for errors.
  * @param timeout is the timeout for processing the whole iterator.
  * @param nwRetry the number of network retries.
  */
case class ResilientGitHubClient(rman: RepoManager
                                 , ds: DataStore
                                 , timeout: Duration
                                 , nwRetry: Int = 1)
  extends RepoManager {
  override def cloneRepo(owner: String, repo: String)
  : Iterator[Try[Git]] = {
    val resAfterRetry = (1 to nwRetry)
      .flatMap(x=> rman.cloneRepo(owner, repo).map(y=> (x, y)))
      .find(x=> x._2.isSuccess || x._1 == nwRetry)
      .get._2

    resAfterRetry match {
      case _@Failure(e) => ds.markRepoError(owner, repo, e.getMessage)
      case _=>
    }
    Iterator(resAfterRetry)
  }

  override def getAllCommits(git: Git)
  : Try[Iterator[CommitMessageRecord]] = {
    val res = rman.getAllCommits(git)

    val (owner, repo) = RepoManager.getRepoOwnership(git)
    res match {
      case _@Failure(e) => ds.markRepoError(owner, repo, e.getMessage)
      case _=>
    }
    res
  }

  override def getAllRevCommits(git: Git)
  : Try[Iterator[RevCommit]] = {
    val res = rman.getAllRevCommits(git)

    val (owner, repo) = RepoManager.getRepoOwnership(git)
    res match {
      case _@Failure(e) => ds.markRepoError(owner, repo, e.getMessage)
      case _=>
    }
    res
  }

  override def getRepoFilesHistory(git: Git, commits: Iterator[RevCommit])
  : Iterator[Try[FileMetadataRecord]] = {
    val (owner, repo) = RepoManager.getRepoOwnership(git)
    val rawResults = rman.getRepoFilesHistory(git, commits)
      .map {
        case failure@Failure(e) =>
          e match {
            case _: IllegalStateException =>
            case _: CheckoutConflictException =>
            case _: MissingObjectException =>
            case _: JGitInternalException =>
          }
          ds.logRepoError(owner, repo, e.toString)
          failure
        case success@_ => success
      }
      .filter(_.isSuccess)

    TimedIterator(rawResults, timeout)
      .map {
        case _@Failure(e)=>
          e match {
            case _:java.util.concurrent.TimeoutException =>
              ds.markRepoError(owner, repo, e.getMessage)
            case _:java.util.NoSuchElementException => /* End record */
          }
          Failure(e)
        case success@_=>success.get
      }
  }

  override def getFileContents(git: Git, objIds: Iterator[String])
  : Iterator[Try[Array[Byte]]] = {
    val (owner, repo) = RepoManager.getRepoOwnership(git)

    val rawResults = rman.getFileContents(git, objIds)
        .map {
          case failure@Failure(e) =>
            ds.markRepoError(owner, repo, e.getMessage)
            failure
          case success@_=> success
        }

    TimedIterator(rawResults, timeout)
      .map {
        case _@Failure(e)=>
          e match {
            case _:java.util.concurrent.TimeoutException =>
              ds.markRepoError(owner, repo, e.getMessage)
            case _:java.util.NoSuchElementException => /* End record */
          }
          Failure(e)
        case success@_=>success.get
      }
  }
}
