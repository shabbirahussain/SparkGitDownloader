package org.reactorlabs.jshealth.repomanagers

import java.io.{File, FileInputStream}
import java.nio.charset.CodingErrorAction
import java.nio.file.Paths
import java.util.Date

import com.google.common.io.Files
import org.apache.commons.codec.digest.DigestUtils
import org.eclipse.jgit.diff.DiffEntry
import org.apache.log4j.Level
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.{CheckoutConflictException, InvalidRemoteException, NoHeadException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.eclipse.jgit.lib.{ObjectId, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.treewalk.{AbstractTreeIterator, CanonicalTreeParser, EmptyTreeIterator}
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.{Codec, Source}

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class GitHubClient(extensions: Set[String], workingGitDir: String, keychain: Keychain)
  extends RepoManager with Serializable {
  private val githubUrl = "https://github.com/"
  private val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)

  private var apiKey   : String   = _
  private var remaining: Int      = 1
  private var reset    : Long     = 0
  private var isValid  : Boolean  = false

  private def getAllCommits(rep: Git): Seq[RevCommit] = {
    var res = Seq[RevCommit]()
    try{
      res = rep.log().call().asScala.toSeq
    } catch {
      case e: NoHeadException => logger.log(Level.WARN, "Empty Repo")
      case e: Exception =>       logger.log(Level.ERROR, e.getMessage)
    }
    res
  }

  override def gitCloneRepo(owner: String, repo: String): Git = {
    var res: Git = null
    val workinDir = Paths.get(workingGitDir + "/" + owner + "/" + repo).toFile
    util.deleteRecursively(workinDir)

    val msg = "\r" + (new Date()) +"\tCloning : %s/%s".format(owner, repo)
    println(("\b" * 200) + msg)

    apiKey = keychain.getNextKey(apiKey, remaining, reset, isValid)
    res = Git.cloneRepository()
        .setDirectory(workinDir)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider("token", apiKey ))
        .setCloneAllBranches(true)
        //.setProgressMonitor()
        .setURI(githubUrl + owner + "/" + repo)
        .call()

    isValid = true
    res
  }

  override def getRepoFilesHistory(git: Git)
  : Seq[FileHashTuple] = {
    def getDiff(oldTreeIter: AbstractTreeIterator, newTreeIter: AbstractTreeIterator)
    : mutable.Buffer[DiffEntry] = {
      git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call().asScala
    }

    val repository  = git.getRepository
    val dir         = repository.getDirectory
    val repo        = dir.getParentFile.getName
    val owner       = dir.getParentFile.getParentFile.getName
    val reader      = repository.newObjectReader
    var oldTreeIter: AbstractTreeIterator = new EmptyTreeIterator()
    var newTreeIter: CanonicalTreeParser  = new CanonicalTreeParser()

    // Process all commits
    val allCommits = getAllCommits(git)
    val cnt = allCommits.length
    val tCnt = (cnt-1)

    val res = allCommits.reverse
      .zipWithIndex.map(x=> {
          if (x._2 % 10 == 0 || x._2 == tCnt){
            val msg = "\r" + (new Date()) + "\t\t\t\t\t\t\tProcessing: %s/%s:  %.2f%% of %7d commits"
              .format(owner, repo, x._2*100.0/tCnt, cnt, x._1.getId.name())
            print(("\b" * msg.length) + msg)
          }
          x._1
      })
      .flatMap(x=> {
        var ret: Seq[FileHashTuple] = Seq()
        try{
          newTreeIter = new CanonicalTreeParser()
          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))

          ret = getDiff(oldTreeIter = oldTreeIter, newTreeIter = newTreeIter)
            .map(y=> {
              if(y.getChangeType == DiffEntry.ChangeType.DELETE)
                (y.getOldPath,  null)
              else
                (y.getNewPath, y.getNewId.name())
            })
            .filter(y=> extensions.contains(Files.getFileExtension(y._1)))
            .map(y => {
              FileHashTuple(owner = owner,
                repo      = repo,
                branch    = x.getTree.getName,
                gitPath   = y._1,
                fileHash  = y._2,
                commitId  = x.getId.name(),
                commitTime= x.getCommitTime)
            })

          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))
          oldTreeIter = newTreeIter
        } catch {
          case e: IllegalStateException     => logger.log(Level.WARN, e.getMessage)
          case e: CheckoutConflictException => logger.log(Level.WARN, e.getMessage)
        }
        ret
      })
    res
  }
}
