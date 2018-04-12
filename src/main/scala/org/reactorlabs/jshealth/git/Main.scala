package org.reactorlabs.jshealth.git

import java.io.File
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util

import scala.concurrent._
import akka.actor._
import akka.dispatch.sysmsg.Terminate
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.{ActorMaterializer, OverflowStrategy, Supervision}
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Sink, Source}
import com.google.common.io.Files
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.reactorlabs.jshealth.util.escapeString

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Random, Try}
/**
  * @author shabbirahussain
  */
object Main {
  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize  = prop.getProperty("git.crawl.batch.size").toInt
  private val numWorkers      = prop.getProperty("git.downloader.numworkers").toInt
  private val clonedRepoBuff  = prop.getProperty("git.downloader.clonedrepo.buffer").toInt
  private val gitPath         = {
    var path = prop.getProperty("git.repo.path")
    if (path.isEmpty) path = Files.createTempDir().getAbsolutePath
    new File(path)
  }

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  // TODO: Add network timeout reload strategy.
  val decider: Supervision.Decider = {
    case _: NullPointerException => Supervision.Stop
    case _: InvalidRemoteException => Supervision.Resume
    case e: TransportException => {
//      if (e.getMessage.contains("400 Bad Request")) Supervision.Resume
//      else Supervision.Restart
      Supervision.Resume
    }
    case _ => Supervision.Resume
  }

  private def getNewGitHubClient(): RepoManager = {
    val map = TrieMap[String, Unit]()
    dataStore.getExistingHashes().foreach(k=> map.put(k, Unit))
    new GitHubClient(
      extensions = extensions,
      keychain   = keychain,
      existingHash  = map ,
      workingGitDir = gitPath.toPath.toAbsolutePath.toString)
  }

  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHistory()
  : Boolean = {
    val (links, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)
    links.persist(StorageLevel.DISK_ONLY)
    if (links.isEmpty()) return false

    val gitHub = getNewGitHubClient()

    val fht = links
      .mapPartitions(x=> {util.deleteRecursively(gitPath); x})
      .mapPartitions(_.grouped(500), preservesPartitioning = true)
      // Progress monitor map stage
      .map(x=> {
          val msg = "\r" + (new Date()) + "\tLoading next batch..."
          println("\b" * 200 + msg)
          logger.log(Level.INFO, msg)
          x
      })
      // Data processing stage
      .flatMap(y=> {
          val future = Source.fromIterator(()=> y.iterator)
            // Clone the git repo
            .mapAsyncUnordered(numWorkers * 2)(x=> Future{
                Try(gitHub.gitCloneRepo(x._1, x._2))
                  match {
                    case _ @ Failure(e) =>
                      // Log all errors to the database
                      logger.log(Level.ERROR, e.getMessage)
                      dataStore.markRepoError(owner = x._1, repo = x._2, branch = x._3, err = e.getMessage)
                      e match {
                        case _: TransportException | _: InvalidRemoteException =>
                        case e: Throwable => e.printStackTrace()
                      }
                      throw e
                    case success @ _ => success.get
                  }
            })
            .withAttributes(supervisionStrategy(decider))
            .buffer(clonedRepoBuff, OverflowStrategy.backpressure)
            // Process the cloned repo
            .mapAsyncUnordered(numWorkers)(git=> Future{
              val commits = Try(gitHub.getRepoFilesHistory(git))
              match {
                case _ @ Failure(e) =>
                  val repo = git.getRepository.getDirectory
                  // Log all errors to the database
                  logger.log(Level.ERROR, e.getMessage)
                  dataStore.markRepoError(owner = repo.getParentFile.getName, repo = repo.getName, branch = "master", err=e.getMessage)
                  Seq.empty
                case success @ _ => success.get
              }

              // Create output splits
              val res = commits
                .filter(_.contents.isDefined)
                .map(x=>
                  (("contents", x.fileHash), """%s""".format(escapeString(x.contents.get)))) // Contents
                .union(commits.map(x=>
                  (("fht"
                    , """%s","%s","%s","%s",%d""".format(x.owner
                      , x.repo
                      , x.gitPath
                      , x.fileHash
                      , x.commitTime))
                    , """"%s",%s,%s""".format(x.commitId
                      , escapeString(x.author)
                      , escapeString(x.longMsg))
                      .replaceAll(""""null"""", ""))
                )) // FileHashTuple
                .union(commits.map(x=>
                  (("commitMsg", """%s""".format(x.commitId))
                    , """%s,%s""".format(escapeString(x.author), escapeString(x.longMsg)))
                )) // Commit Messages
                .union(commits.map(x=>
                  (("indexes", x.fileHash), ""))) // Indexes

              // Delete finished repos
              if(res.count(_=> true) >= 0) // Force all data to be generated before deleting repo
                  util.deleteRecursively(git.getRepository.getDirectory.getParentFile)
              res
            })
            .runWith(Sink.seq)
          Await.result(future, Duration.Inf).flatten
        })

    dataStore.storeHistory(fht, token.toString)
    dataStore.markRepoCompleted(links.map(x=> (x._1, x._2, x._3)).distinct)

    links.unpersist(blocking = false)
    true
  }

  def main(args: Array[String])
  : Unit = {
    println("Git.Main")

    var continue = false
    do{
      continue = crawlFileHistory()
    } while(continue)
//    system.terminate()
  }
}
