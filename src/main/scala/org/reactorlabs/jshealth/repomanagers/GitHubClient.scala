package org.reactorlabs.jshealth.repomanagers

import java.io.{File, FileInputStream}
import java.nio.charset.CodingErrorAction
import java.nio.file.Paths

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
  private var remaining: Int      = 0
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

  private def gitCloneRepo(owner: String, repo: String)
  : (Git, File) = {
    var res: Git = null
    val workinDir = Paths.get(workingGitDir + "/" + owner + "/" + repo).toFile
    util.deleteRecursively(workinDir)

    apiKey = keychain.getNextKey(apiKey, remaining, reset, isValid)
    res = Git.cloneRepository()
        .setDirectory(workinDir)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider( "token", apiKey ))
        .setCloneAllBranches(true)
        //      .setProgressMonitor()
        .setURI(githubUrl + owner + "/" + repo)
        .call()

    isValid = true
    (res, workinDir)
  }

  private def getRepoFilesHistory(git: Git)
  : Seq[FileHashTuple] = {
    def getDiff(oldTreeIter: AbstractTreeIterator, newTreeIter: AbstractTreeIterator)
    : mutable.Buffer[DiffEntry] = {
      git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call().asScala
    }

    val repository  = git.getRepository
    val reader      = repository.newObjectReader
    var oldTreeIter: AbstractTreeIterator = new EmptyTreeIterator()
    var newTreeIter: CanonicalTreeParser  = new CanonicalTreeParser()

    // Get all interesting files from head. We will only track history of these files.
    newTreeIter.reset(reader, repository.resolve("HEAD^{tree}"))
    val headFiles = getDiff(oldTreeIter = oldTreeIter, newTreeIter = newTreeIter)
      .map(x=> x.getNewPath)
      .filter(x=> extensions.contains(Files.getFileExtension(x)))
      .toSet
    if (headFiles.isEmpty) throw new Exception("No interesting files found in head. Skipping.")

    // Process all commits
    val allCommits = getAllCommits(git)
    val cnt = allCommits.length

    val res = allCommits.reverse.zipWithIndex
      .map(x=> {
          val msg = "\tProcessing:  %.2f%% of %7d commits. Current Commit=%s".format(x._2*100.0/cnt, cnt, x._1.getId.name())
          print(("\b" * msg.length) + msg)
          x._1
      })
      .flatMap(x=> {
        var ret: Seq[FileHashTuple] = Seq()
        try{
          newTreeIter = new CanonicalTreeParser()
          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))

          val diffs = getDiff(oldTreeIter = oldTreeIter, newTreeIter = newTreeIter)

          ret = diffs
            .filter(x=> (x.getChangeType == DiffEntry.ChangeType.ADD) || (x.getChangeType == DiffEntry.ChangeType.MODIFY))
            .filter(x=> headFiles.contains(x.getNewPath))
            .map(x=> (x.getNewPath , x.getNewId.name()))
            .map(y => {
              FileHashTuple(owner = null,
                repo      = null,
                branch    = x.getTree.getName,
                gitPath   = y._1,
                fileType  = FileTypes.blob,
                fileHash  = y._2,
                byteSize  = -1,
                commitId  = x.getId.name(),
                commitMsg = x.getShortMessage,
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

  override def getFileCommits(owner: String, repo: String, branch: String)
  : (Seq[FileHashTuple], Option[File], String) = {
    var res:  Seq[FileHashTuple] = Seq()
    var file: Option[File] = None
    var msg:  String = null
    try{
      print("\tCloning")
      val (git, foldr) = gitCloneRepo(owner, repo)
      file = Some(foldr)

      print("\tProcessing")
      res = getRepoFilesHistory(git)
          .map (x=> FileHashTuple(
            owner     = owner,
            repo      = repo,
            branch    = x.branch,
            gitPath   = x.gitPath,
            fileType  = x.fileType,
            fileHash  = x.fileHash,
            byteSize  = x.byteSize,
            commitId  = x.commitId,
            commitMsg = x.commitMsg.toLowerCase(),
            commitTime= x.commitTime,
            isBugFix  = x.commitMsg.toLowerCase().contains("fix")
          ))

    } catch{
      case e:NoRemoteRepositoryException => {
        msg = e.getMessage
        logger.log(Level.ERROR, e.getMessage)
      }
      case e:InvalidRemoteException =>{
        msg = e.getMessage
        logger.log(Level.ERROR, e.getMessage)
      }
      case e:Exception => logger.log(Level.ERROR, e.getMessage)
        e.printStackTrace()
        msg = e.getMessage
        logger.log(Level.ERROR, e.getMessage)
    }
    (res, file, msg)
  }
}
