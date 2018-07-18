package org.reactorlabs.jshealth.repomanagers

import java.nio.file.Paths

import org.eclipse.jgit.diff.DiffEntry
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.treewalk.{AbstractTreeIterator, CanonicalTreeParser, EmptyTreeIterator}
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{CommitMessageRecord, FileMetadataRecord}

import scala.collection.JavaConverters._
import scala.collection.{SortedMap, mutable}
import scala.util.{Failure, Success, Try}

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @param keychain is the keychain enabling github authentication.
  * @param workingGitDir is the temporary directory to use for cloning.
  * @param cloningTimeout is the max time in sec to timeout for connections of cloning.
  *
  * @author shabbirahussain
  */
case class BaseGitHubClient(keychain: Keychain, workingGitDir: String, cloningTimeout: Int)
  extends RepoManager {
  private val githubUrl = "https://github.com"

  override def cloneRepo(owner: String, repo: String)
  : Iterator[Try[Git]] = Iterator(Try {
    val workinDir = Paths.get("%s/%s/%s".format(workingGitDir, owner, repo)).toFile

    Git.cloneRepository()
      .setDirectory(workinDir)
      .setCredentialsProvider(new UsernamePasswordCredentialsProvider("token", keychain.getNextKey()))
      .setCloneAllBranches(true)
      .setURI("%s/%s/%s".format(githubUrl, owner, repo))
      .setTimeout(cloningTimeout)
      .call()
  })

  override def getAllRevCommits(git: Git)
  : Try[Iterator[RevCommit]] = Try {
    git.log().call().asScala
      .toSeq.reverse
      .toIterator
  }

  override def getAllCommits(git: Git)
  : Try[Iterator[CommitMessageRecord]] = getAllRevCommits(git) match {
    case _@Failure(e)=> Failure(e)
    case success@_=>
      val (owner, repo) = RepoManager.getRepoOwnership(git)
      Try{ success.get.map(x=> CommitMessageRecord(
          repo_owner = owner,
          repository = repo,
          branch = x.getTree.name,
          author = x.getAuthorIdent.getEmailAddress,
          commit_id = x.getId.getName,
          commit_time = x.getCommitTime,
          commit_message = x.getFullMessage)
      )}
  }

  override def getRepoFilesHistory(git: Git, commits: Iterator[RevCommit])
  : Iterator[Try[FileMetadataRecord]] = {
    def getDiff(oldTreeIter: AbstractTreeIterator, newTreeIter: AbstractTreeIterator)
    : mutable.Buffer[DiffEntry] = {
      git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call().asScala
    }

    val (repo, owner) = RepoManager.getRepoOwnership(git)
    val repository  = git.getRepository
    val reader      = repository.newObjectReader
    var oldTreeIter: AbstractTreeIterator = new EmptyTreeIterator()
    var newTreeIter: CanonicalTreeParser  = new CanonicalTreeParser()

    // Process all commits
    commits
      .flatMap(x=> Try {
          newTreeIter = new CanonicalTreeParser()
          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))

          val ret = getDiff(oldTreeIter = oldTreeIter, newTreeIter = newTreeIter)
            .map(y=> if(y.getChangeType == DiffEntry.ChangeType.DELETE)
                (y.getOldPath,  null)
              else
                (y.getNewPath, y.getNewId.name())
            )
            .map(y=> FileMetadataRecord(repo_owner = owner,
                repository = repo,
                branch     = x.getTree.getName,
                git_path   = y._1,
                file_hash  = y._2,
                commit_id  = x.getId.name(),
                commit_time= x.getCommitTime)
            )

          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))
          oldTreeIter = newTreeIter
          ret
        } match {
          case _@Failure(e) => Iterator(Failure(e))
          case success@_ => success.get.map(Success(_))
        }
      )
  }

  override def getFileContents(git: Git, objIds: Iterator[String])
  : Iterator[Try[Array[Byte]]] = objIds
    .map(x=> ObjectId.fromString(x))
    .map(x=> Try {git.getRepository.open(x).getBytes})
}
