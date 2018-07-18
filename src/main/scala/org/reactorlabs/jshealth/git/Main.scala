package org.reactorlabs.jshealth.git

import java.io.File
import java.lang.Throwable
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{BaseGitHubClient, RepoManager, ResilientGitHubClient}
import org.reactorlabs.jshealth.util._
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.reactorlabs.jshealth.models.{CommitMessageRecord, FileMetadataRecord, Schemas}
import org.apache.hadoop.fs.Path
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.RevCommit

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}
/**
  * @author shabbirahussain
  */
object Main {
  import sqlContext.implicits._

  private val metaExtns       = prop.getProperty("git.download.metadata.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val contentsExtn    = prop.getProperty("git.download.contents.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize  = prop.getProperty("git.downloader.crawl.batch.size").toInt
//  private val nWorkers        = prop.getProperty("git.downloader.numworkers").toInt
  private val processingTO = prop.getProperty("git.downloader.processing.timeout.sec").toLong
  private val partitionSize   = prop.getProperty("git.downloader.partition.size").toInt
  private val gitPath    = {
    var cloningTempDir = prop.getProperty("git.downloader.cloning.temp.dir")
    if (cloningTempDir == null)
      cloningTempDir = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp") + "/repos/"
    new File( "%s%d".format(cloningTempDir, System.currentTimeMillis()))
  }
  private val gitClient : RepoManager = {
    val cloningTimeout  = prop.getProperty("git.downloader.cloning.timeout.sec").toInt
    val timeout = Duration.create(processingTO, TimeUnit.SECONDS)
    val base = BaseGitHubClient(
      keychain      = keychain,
      workingGitDir = gitPath.toPath.toAbsolutePath.toString,
      cloningTimeout
    )
    ResilientGitHubClient(base, ds, timeout, nwRetry = 3)
  }



  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlCommitMessages()
  : Boolean = {
    logger.log(Level.INFO, "\n\n\nChecking out links ...")
    val (links, token) = ds.checkoutReposToCrawl(crawlBatchSize)
    links.persist(StorageLevel.DISK_ONLY)
    if (links.isEmpty()) return false

    logger.log(Level.INFO, "Initializing ...")
    val data = links
      .repartition(crawlBatchSize / partitionSize )
      // Clone git repo
      .flatMap(x=> gitClient
        .cloneRepo(x._1, x._2)
        .flatMap {
          case _@Failure(_) => Iterator.empty
          case success@_ => {
            val git = success.get
            def cleanUpRepo(): Unit = fs.delete(new Path(git.getRepository.getDirectory.getParentFile.toURI), true)

            gitClient.getAllCommits(git)
            match {
              case _@Failure(_) => Iterator.empty
              case success@_ =>
                ExecuteAfterLastElemIterator(success.get, cleanUpRepo)

            }
          }
        }).toDF()
    ds.storeRecords(data, folder = "%d/%s".format(token, Schemas.COMMIT_MESSAGES))
    fs.delete(new Path(gitPath.toURI), true)

    ds.markRepoCompleted(links.map(x=> (x._1, x._2)), token)
    links.unpersist(blocking = false)
    true
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
      .repartition(crawlBatchSize / partitionSize )
      // Clone git repo
      .flatMap(x=> gitClient
        .cloneRepo(x._1, x._2)
        .flatMap {
          case _@Failure(_) => Iterator.empty
          case success@_ =>
            val git = success.get
            def cleanUpRepo(): Unit = fs.delete(new Path(git.getRepository.getDirectory.getParentFile.toURI), true)

            gitClient.getAllRevCommits(git)
            match {
              case _@Failure(_) => Iterator.empty
              case success@_ =>
                val commits = ExecuteAfterLastElemIterator(success.get, cleanUpRepo)

                gitClient
                  .getRepoFilesHistory(git, commits)
                  .filter(_.isSuccess).map(_.get)
                  .filter(x=> metaExtns.isEmpty || metaExtns.contains(scala.reflect.io.File(x.git_path).extension))
            }
      }).toDF()
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
//      continue = crawlFileHistory()
      continue = crawlCommitMessages()
    } while(!continue)
  }
}
