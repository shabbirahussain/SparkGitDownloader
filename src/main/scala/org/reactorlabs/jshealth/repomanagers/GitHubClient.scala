package org.reactorlabs.jshealth.repomanagers

import java.io.File
import java.nio.file.Paths

import com.google.common.io.Files
import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Level
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.NoHeadException
import org.eclipse.jgit.revwalk.RevCommit
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.util

import scala.collection.JavaConverters._
import scala.io.Source

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class GitHubClient(extensions: Set[String], workingGitDir: String)
  extends RepoManager with Serializable {
  private val githubUrl = "https://github.com/"

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
    val workinDir = Paths.get(workingGitDir + "/" + owner + "/" + repo).toFile
    util.deleteRecursively(workinDir)

    val rep: Git = Git.cloneRepository()
      .setDirectory(workinDir)
      .setCloneAllBranches(true)
//      .setProgressMonitor()
      .setURI(githubUrl + owner + "/" + repo)
      .call()
    (rep, workinDir)
  }

  private def getRepoFilesHistory(git: Git)
  : Seq[FileHashTuple] = {
    var fileMap:Set[Object] = Set()
    def getModifiedFiles(file: File) : Seq[(String, String, Long)] = {
      def makeFileKey(file: File): Object = (file.getPath, file.lastModified())

      val homePathLen = file.getAbsolutePath.length + 1

      val files = util.recursiveListFiles(file)
        .filter(_.isFile)
        .filter(x=> extensions.contains(Files.getFileExtension(x.getName)))
        .filter(x=> !fileMap.contains(makeFileKey(x))) // Select only new or modified files.
      fileMap = files.map(makeFileKey).toSet

      val res = files
        .map(x=> {
          val hashCode = DigestUtils.sha1Hex(Source.fromFile(x).mkString)
          val gitPath  = x.getAbsolutePath.substring(homePathLen)
          val size     = x.length()
          (gitPath, hashCode, size)
        })
      res.toSeq
    }

    print("\t"+ ("_"*40))
    val res = getAllCommits(git)
      .reverse
      .flatMap(x=> {
        print(("\b"*40) + x.getId.name())

        // Checkout repo commmit
        git.checkout()
//          .setCreateBranch(true)
          .setName(x.getId.name())
          .call()

        // build history tuple
        getModifiedFiles(git.getRepository.getDirectory.getParentFile)
          .map(y=> {
            FileHashTuple(owner = null,
              repo      = null,
              branch    = x.getTree.getName,
              gitPath   = y._1,
              fileType  = FileTypes.blob,
              fileHash  = y._2,
              byteSize  = y._3,
              commitId  = x.getId.name(),
              commitMsg = x.getShortMessage,
              commitTime= x.getCommitTime)
          })
      })
    res
  }


  override def getFileCommits(owner: String, repo: String, branch: String)
  : (Seq[FileHashTuple], File) = {
    print("\tCloning")
    val (git, file) = gitCloneRepo(owner, repo)

    print("\tProcessing")

    val res = getRepoFilesHistory(git)
      .map (x=> {
        x.owner  = owner
        x.repo   = repo
        x
      })

    (res, file)
  }
}
