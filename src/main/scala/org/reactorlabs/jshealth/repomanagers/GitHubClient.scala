package org.reactorlabs.jshealth.repomanagers

import java.io.File
import java.nio.file.Paths

import com.google.common.io.Files
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevCommit
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

  private def getAllCommits(rep: Git): Seq[RevCommit] = rep.log().call().asScala.toSeq

  private def gitCloneRepo(owner: String, repo: String, branch: String)
  : Git = {
    val workinDir = Paths.get(workingGitDir + "/" + owner + "/" + repo).toFile
    util.deleteRecursively(workinDir)

    val rep: Git = Git.cloneRepository()
      .setBranch(branch)
      .setDirectory(workinDir)
//      .setCloneAllBranches(true)
//      .setProgressMonitor()
      .setURI(githubUrl + owner + "/" + repo)
      .call()
    rep
  }
  private def getRepoFilesHistory(git: Git)
  : Seq[FileHashTuple] = {
    var fileMap:Set[Object] = Set()
    def getModifiedFiles(file: File) : Seq[(String, Int, Long)] = {
      def makeFileKey(file: File): Object = (file.getPath, file.lastModified())

      val homePathLen = file.getAbsolutePath.length

      val files = util.recursiveListFiles(file)
        .filter(_.isFile)
        .filter(x=> extensions.contains(Files.getFileExtension(x.getName)))
        .filter(x=> !fileMap.contains(makeFileKey(x))) // Select only new or modified files.
      fileMap = files.map(makeFileKey).toSet

      val res = files
        .map(x=> {
          val hashCode = Source.fromFile(x).mkString.hashCode
          val gitPath  = x.getAbsolutePath.substring(homePathLen)
          val size = x.getTotalSpace
          (gitPath, hashCode, size)
        })
      res
    }


    val res = getAllCommits(git).flatMap(x=> {
      // Checkout repo commmit
      git.checkout().setName(x.getId.name())

      // build history tuple
      getModifiedFiles(git.getRepository.getDirectory)
        .map(y=> {
          FileHashTuple(owner = "",
            repo      = "",
            branch    = "master",
            gitPath   = "",
            fileType  = FileTypes.blob,
            fileHash  = y._2.toString,
            commitId  = x.getId.name(),
            commitMsg = x.getShortMessage,
            commitTime= x.getCommitTime)
        })
    })
    res
  }


  override def getFileCommits(owner: String, repo: String, branch: String): Seq[FileHashTuple] = {
    val git = gitCloneRepo(owner, repo, branch)
    getRepoFilesHistory(git)
  }
}
