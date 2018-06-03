package org.reactorlabs.jshealth.git

import java.io.File
import java.util.concurrent.Executors

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util._
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.reactorlabs.jshealth.models.{FileHashTuple, Schemas}
import org.apache.hadoop.fs.Path
import org.eclipse.jgit.api.Git

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}
/**
  * @author shabbirahussain
  */
object Main {
  import sqlContext.implicits._

  private val metaExtns       = prop.getProperty("git.download.metadata.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val contentsExtn    = prop.getProperty("git.download.contents.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize  = prop.getProperty("git.crawl.batch.size").toInt
  private val groupSize       = prop.getProperty("git.downloader.group.size").toInt
  private val nWorkers        = prop.getProperty("git.downloader.numworkers").toInt
  private val clonedRepoBuff  = prop.getProperty("git.downloader.repo.buffer").toInt
  private val consolFreq      = prop.getProperty("git.downloader.consolidation.frequency").toInt
  private val genDataFor      = prop.getProperty("git.generate.data.for")
    .toUpperCase.split(",").map(_.trim)
    .toSet
    .map(Schemas.withName)
  private val gitPath    = {
    var cloningTempDir = prop.getProperty("git.downloader.cloning.temp.dir")
    if (cloningTempDir == null)
      cloningTempDir = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp") + "/repos/"
    new File( "%s%d".format(cloningTempDir, System.currentTimeMillis()))
  }
  private val cloningTimeout  = prop.getProperty("git.downloader.cloning.timeout.sec").toInt
  private val gitClient : RepoManager = new GitHubClient(
      keychain   = keychain,
      workingGitDir = gitPath.toPath.toAbsolutePath.toString,
      cloningTimeout
  )
  private val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nWorkers))

  /**
    * @param git is the cloned git repo object to fetch details from.
    * @return Owner of the repo and repository name.
    */
  private def getRepoOwnership(git: Git): (String, String) = {
    val dir = git.getRepository.getDirectory.getParentFile
    (dir.getParentFile.getName, dir.getName)
  }

  /** Clones the repo using a github client. If unsuccessful marks error in the database and returns empty.
    *
    * @param owner is the owner of the repository.
    * @param repo is the repository name.
    * @param token is the token id of the process.
    * @return A single element seq of Git object if successful.
    */
  private def tryCloneRepo(owner: String, repo: String, token: Long)
  = {
    Try(gitClient.gitCloneRepo(owner = owner, repo = repo))
    match {
      case _ @ Failure(e) =>
        ds.markRepoError(owner, repo, err = e.getMessage, token = token)
        e match {
          case _: TransportException | _: InvalidRemoteException =>
          case e: Throwable => e.printStackTrace()
        }
        Seq.empty
      case success @ _ => Seq(success.get)
    }
  }

  /** Tries to get file history from the git repository.
    *
    * @param git is the cloned git repo object to fetch history from.
    * @param token is the token id of the process.
    * @return A seq of FileHashTuples.
    */
  private def tryGetRepoFilesHistory(git: Git, token: Long)
  : Seq[FileHashTuple] = {
    val (owner, repo) = getRepoOwnership(git)
    Try(gitClient.getRepoFilesHistory(git))
    match {
      case _@Failure(e) =>
        ds.markRepoError(owner, repo, err = e.getMessage, token = token)
        Seq.empty
      case success@_ => success.get
    }
  }

  /** Tries to fetch contents form the cloned github repo. Returns only the 'UTF-8' string contents.
    *
    * @param objectId is the id of the object to fetch contents from.
    * @param git is the cloned git repository object.
    * @param token is the token id of the process.
    * @return Seq of just one element tuple containing fileHash and UTF-8 String contents if successful.
    */
  private def tryGetFileContents(objectId: String, git: Git, token: Long)
  : Seq[(String, String)] = {
    val (owner, repo) = getRepoOwnership(git)
    Try((objectId, new String(gitClient.getFileContents(git, objectId),"UTF-8")))
    match {
      case _ @ Failure(e) =>
        ds.markRepoError(owner, repo, err = e.getMessage, token = token)
        Seq.empty
      case success @ _ => Seq(success.get)
    }
  }

  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHistory()
  : Boolean = {
    logger.log(Level.INFO, "\n\n\nChecking out links ...")
    val (links, token) = ds.checkoutReposToCrawl(crawlBatchSize)
    links.persist(StorageLevel.DISK_ONLY)
    if (links.isEmpty()) return false

    logger.log(Level.INFO, "Initializing ...")
    val data = links
      .repartition(crawlBatchSize / groupSize)
      .flatMap(x=> tryCloneRepo(x._1, x._2, token))// Clone git repo
      .flatMap(git=> {
        val data = tryGetRepoFilesHistory(git, token).toIterator // Extract data from it
        data.map(y=> {
          if(data.isEmpty)
            fs.delete(new Path(git.getRepository.getDirectory.getParentFile.toURI), true)
          y
        })
      })
      .filter(fht=> metaExtns.isEmpty || metaExtns.contains(scala.reflect.io.File(fht.gitPath).extension))
      .map(x=> (x.owner, x.repo, x.gitPath, x.fileHash, x.commitTime, x.commitId))
      .toDF(Schemas.asMap(Schemas.FILE_METADATA)._3:_*)
    ds.storeRecords(data, folder = "%d/%s".format(token, Schemas.FILE_METADATA))
    fs.delete(new Path(gitPath.toURI), true)

    ds.markRepoCompleted(links.map(x=> (x._1, x._2)), token)
    links.unpersist(blocking = false)
    true
  }

  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileContents()
  : Boolean = {
    throw new RuntimeException("Not Implemented Yet")

    false
  }

  def main(args: Array[String])
  : Unit = {
    logger.log(Level.INFO, "Git.Main")
    logger.log(Level.DEBUG, "Cloning repos in [%s]".format(gitPath.toPath.toAbsolutePath))

    var continue = false
    do{
      continue = crawlFileHistory()
    } while(continue)
  }
}
