package org.reactorlabs.jshealth.git

import java.io.File
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util

import scala.concurrent._
import akka.actor._
import akka.dispatch.sysmsg.Terminate
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.{ActorMaterializer, OverflowStrategy, Supervision}
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Sink, Source}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Try}
/**
  * @author shabbirahussain
  */
object Main {
  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val gitPath         = prop.getProperty("git.repo.path")
  private val crawlBatchSize  = prop.getProperty("git.crawl.batch.size").toInt
  private val numWorkers      = prop.getProperty("git.downloader.numworkers").toInt
  private val clonedRepoBuff  = prop.getProperty("git.downloader.clonedrepo.buffer").toInt
  private val gitHub: RepoManager  = new GitHubClient(extensions = extensions, gitPath, keychain)

  implicit val system: ActorSystem = ActorSystem("QuickStart")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val decider: Supervision.Decider = {
    case _: NullPointerException => Supervision.Stop
    case _: InvalidRemoteException => Supervision.Resume
    case e: TransportException => {
      if (e.getMessage.contains("400 Bad Request")) Supervision.Resume
      else Supervision.Restart
    }
    case _ => Supervision.Resume
  }

  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHistory()
  : Boolean = {
    val (links, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)

    val fht = links
      .mapPartitions(_.grouped(100), preservesPartitioning = true)
      .map(x=> {
          val msg = "\r" + (new Date()) + "\tLoading next batch..."
          println("\b" * 200 + msg)
          logger.log(Level.INFO, msg)
          x
      })
      .flatMap(y=> {
          val future: Future[Seq[Seq[FileHashTuple]]] = Source.fromIterator(()=> y.iterator)
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
            }).withAttributes(supervisionStrategy(decider))
            .buffer(clonedRepoBuff, OverflowStrategy.backpressure)
            // Process the cloned repo
            .mapAsyncUnordered(numWorkers)(git=> Future{
              Try((gitHub.getRepoFilesHistory(git), git.getRepository.getDirectory.getParentFile))
              match {
                case _ @ Failure(e) =>
                  val repo = git.getRepository.getDirectory
                  // Log all errors to the database
                  logger.log(Level.ERROR, e.getMessage)
                  dataStore.markRepoError(owner = repo.getParentFile.getName, repo = repo.getName, branch = "master", err=e.getMessage)
                  throw e
                case success @ _ => success.get
              }
            }).withAttributes(supervisionStrategy(decider))
            // Delete finished repos
            .map(x=> {
              if (x._1.count(_=> true) > 0)
                util.deleteRecursively(x._2)
              x._1
            })
            .runWith(Sink.seq)
          Await.result(future, Duration.Inf).flatten
        })
    dataStore.storeHistory(fht, token.toString)
    dataStore.markRepoCompleted(fht.map(x=> (x.owner, x.repo, x.branch)).distinct)

    (links.count() > 0)
  }

  def main(args: Array[String])
  : Unit = {
    println("Git.Main")

    var continue = true
    do{
      util.deleteRecursively(Paths.get(gitPath).toFile)
      continue = crawlFileHistory()
    } while(continue)

//
//    val (files, folder, errmsg) = gitHub.getFileCommits("shabbirahussain", "SparkTest", "master")
//    val (files, folder, errmsg) = gitHub.getFileCommits("aFarkas", "webshim", "master")
//    println("\n" + files.length)
//    dataStore.storeHistory(files)
//    files.foreach(println)
//
//    util.deleteRecursively(folder)
  }
}
