package org.reactorlabs.jshealth.repomanagers

import java.io.File
import java.nio.charset.CodingErrorAction
import java.nio.file.Paths

import com.google.common.io.Files
import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.Level
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.{InvalidRemoteException, NoHeadException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.transport.{CredentialItem, CredentialsProvider, URIish, UsernamePasswordCredentialsProvider}
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.util

import scala.collection.JavaConverters._
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
          val hashCode = DigestUtils.sha1Hex(Source.fromFile(x)(decoder).mkString)
          val gitPath  = x.getAbsolutePath.substring(homePathLen)
          val size     = x.length()
          (gitPath, hashCode, size)
        })
      res.toSeq
    }

    print("\t"+ ("_" * 40))
    val res = getAllCommits(git)
      .reverse
      .flatMap(x=> {
        print(("\b"*40) + x.getId.name())
        var ret: Seq[FileHashTuple] = Seq()
        try{
          // Checkout repo commmit
          git.checkout()
            //.setCreateBranch(true)
            .setName(x.getId.name())
            .call()

          // build history tuple
          ret = getModifiedFiles(git.getRepository.getDirectory.getParentFile)
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
        } catch {
          case e: java.lang.IllegalStateException => logger.log(Level.WARN, e.getMessage)
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
            commitMsg = x.commitMsg,
            commitTime=x.commitTime
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
        System.exit(1)
    }
    (res, file, msg)
  }
}
