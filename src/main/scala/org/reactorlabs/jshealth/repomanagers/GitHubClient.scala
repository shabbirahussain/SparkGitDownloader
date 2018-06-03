package org.reactorlabs.jshealth.repomanagers

import java.nio.file.Paths
import java.util.Date

import com.google.common.io.Files
import org.eclipse.jgit.diff.DiffEntry
import org.apache.log4j.Level
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.{CheckoutConflictException, InvalidRemoteException, JGitInternalException, NoHeadException}
import org.eclipse.jgit.lib.ObjectId
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.treewalk.{AbstractTreeIterator, CanonicalTreeParser, EmptyTreeIterator}
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.FileHashTuple

import scala.collection.JavaConverters._
import org.eclipse.jgit.errors.MissingObjectException

import scala.collection.concurrent.TrieMap
import scala.collection.{SortedMap, mutable}
import scala.util.{Failure, Try}

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @param keychain is the keychain enabling github authentication.
  * @param workingGitDir is the temporary directory to use for cloning.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class GitHubClient(keychain: Keychain, workingGitDir: String, cloningTimeout: Int)
  extends RepoManager with Serializable {
  private val githubUrl = "https://github.com"
//  private val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)

  private var apiKey   : String   = _
  private var remaining: Int      = 1
  private var reset    : Long     = 0
  private var isValid  : Boolean  = true

  private val columnWidths = Seq("Cloning:", "%-20.20s", "Processing:", "%-20.20s", "%8.8s", "%5s")
  private def showStatus(values: Map[Int, String]): Unit = {
    val msg = new Date() + " " + columnWidths
      .zipWithIndex
      .map(x=> x._1.format(values.getOrElse(x._2, "")))
      .mkString(" ")
    print("%s\r%s".format("\b" * msg.length, msg))
  }

  private def getAllCommits(git: Git): Seq[RevCommit] = {
    Try(git.log().call().asScala.toSeq)
    match {
      case _ @ Failure(e) =>
      e match{
        case e: NoHeadException => logger.log(Level.WARN, "Empty Repo")
        case e: Exception =>       logger.log(Level.ERROR, e.getMessage)
      }
      Seq.empty
      case success @ _ => success.get
    }
  }

  override def gitCloneRepo(owner: String, repo: String): Git = {
    var res: Git = null
    val workinDir = Paths.get("%s/%s/%s".format(workingGitDir, owner, repo)).toFile

    showStatus(Map(1-> "%s/%s".format(owner, repo), 3-> "\n"))
    apiKey = keychain.getNextKey(apiKey, remaining, reset, isValid)
    res = Git.cloneRepository()
        .setDirectory(workinDir)
        .setCredentialsProvider(new UsernamePasswordCredentialsProvider("token", apiKey ))
        .setCloneAllBranches(true)
        .setURI("%s/%s/%s".format(githubUrl, owner, repo))
        .setTimeout(cloningTimeout)
        .call()

    isValid = true
    res
  }

  override def getFileContents(git: Git, objId: String): Array[Byte] = {
    git.getRepository.open(ObjectId.fromString(objId)).getBytes
  }

  override def getRepoFilesHistory(git: Git)
  : Seq[FileHashTuple] = {
    def getDiff(oldTreeIter: AbstractTreeIterator, newTreeIter: AbstractTreeIterator)
    : mutable.Buffer[DiffEntry] = {
      git.diff.setNewTree(newTreeIter).setOldTree(oldTreeIter).call().asScala
    }

    val repository  = git.getRepository
    val repo        = repository.getDirectory.getParentFile.getName
    val owner       = repository.getDirectory.getParentFile.getParentFile.getName
    val reader      = repository.newObjectReader
    var oldTreeIter: AbstractTreeIterator = new EmptyTreeIterator()
    var newTreeIter: CanonicalTreeParser  = new CanonicalTreeParser()

    // Process all commits
    val allCommits = getAllCommits(git)
    val cnt = allCommits.length
    val tCnt = cnt - 1

    val res = allCommits.reverse
      // Progress monitor
      .zipWithIndex.map(x=> {
          if (x._2 % 100 == 0)
            showStatus(
              Map(3-> "%s/%s"  .format(owner, repo)
                , 4-> "%2.2f%%".format((x._2 + 1)*100.0/cnt)
                , 5-> "of %7d" .format(cnt)))
          else if ( x._2 == tCnt)
            showStatus(
              Map(3-> "%s/%s"  .format(owner, repo)
                , 4-> "%2.2f%%".format((x._2 + 1)*100.0/cnt)
                , 5-> "of %7d\n" .format(cnt)))
          x._1
      })
      .flatMap(x=> {
        Try{
          newTreeIter = new CanonicalTreeParser()
          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))

          val ret = getDiff(oldTreeIter = oldTreeIter, newTreeIter = newTreeIter)
            .map(y=> if(y.getChangeType == DiffEntry.ChangeType.DELETE)
                (y.getOldPath,  null)
              else
                (y.getNewPath, y.getNewId.name())
            )
            .map(y=> FileHashTuple(owner = owner,
                repo      = repo,
                branch    = x.getTree.getName,
                gitPath   = y._1,
                fileHash  = y._2,
                commitId  = x.getId.name(),
                commitTime= x.getCommitTime)
            )

          newTreeIter.reset(reader, repository.resolve(x.getTree.getName))
          oldTreeIter = newTreeIter
          ret
        } match {
          case _@Failure(e) =>
            e match {
              case _: IllegalStateException =>
              case _: CheckoutConflictException =>
              case _: MissingObjectException =>
              case _: JGitInternalException =>
            }
            //logger.log(Level.WARN, e.getMessage)
            Seq.empty
          case success@_ => success.get
        }
      })
    res
  }
}
