package org.reactorlabs.jshealth.repomanagers

import java.io.File
import java.text.SimpleDateFormat

import com.google.common.io.Files
import com.google.gson.Gson
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Level
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevCommit
import org.reactorlabs.jshealth.Main.logger
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.util

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API.
  *
  * @author shabbirahussain
  */
@SerialVersionUID(100L)
class GitHubClient()
  extends RepoManager with Serializable {
  val githubUrl = "https://github.com/"

  var file

  def gitCloneRepo(repoOwner: String, repository: String, branch: String)
  : Unit = {
    val temp = Files.createTempDir()
    val rep: Git = Git.cloneRepository()
      .setBranch(branch)
      .setDirectory(temp)
//      .setCloneAllBranches(true)
//      .setProgressMonitor()
      .setURI(githubUrl + repoOwner + "/" + repository + ".git")
      .call()

  }

  def getAllCommits(rep: Git): Seq[RevCommit] = rep.log().call().asScala.toSeq

  def getRepoFilesHistory(rep: Git, commits: Seq[RevCommit])
  : Seq[FileHashTuple] = {
    commits.flatMap(x=> {
      rep.checkout().setName(x.getId)

      (x.getId, x.getShortMessage)
    })
  }

  def getFileHistory(file: File, commit: RevCommit) = {
//    rep.getRepository.getFS.

    util.recursiveListFiles(file)
      .map(x=> x.)
  }

}
