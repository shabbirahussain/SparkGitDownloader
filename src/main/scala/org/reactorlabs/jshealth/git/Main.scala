package org.reactorlabs.jshealth.git

import java.io.File
import java.util.Date
import java.util.concurrent.Executors

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util

import scala.concurrent._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.ActorMaterializer
import akka.stream.Supervision.resumingDecider
import akka.stream.scaladsl.{Sink, Source}
import org.eclipse.jgit.api.errors.InvalidRemoteException
import org.eclipse.jgit.errors.NoRemoteRepositoryException

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
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
  private val gitHub: RepoManager  = new GitHubClient(extensions = extensions, gitPath, keychain)

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()

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
            .mapAsyncUnordered(numWorkers * 2)(x=>   Future{
              try{
                gitHub.gitCloneRepo(x._1, x._2)
              } catch {
                case e: Exception =>
                  logger.log(Level.ERROR, e.getMessage)
                  dataStore.markRepoError(owner = x._1, repo = x._2, branch = x._3, err=e.getMessage)
                  throw e
              }
            }).withAttributes(supervisionStrategy(resumingDecider))
            .mapAsyncUnordered(numWorkers)(git=> Future{
              try{
                (gitHub.getRepoFilesHistory(git), git.getRepository.getDirectory.getParentFile)
              } catch {
                case e: Exception =>
                  val repo = git.getRepository.getDirectory
                  logger.log(Level.ERROR, e.getMessage)
                  dataStore.markRepoError(owner = repo.getParentFile.getName, repo = repo.getName, branch = "master", err=e.getMessage)
                  throw e
              }
            }).withAttributes(supervisionStrategy(resumingDecider))
            .map(x=> {
              if (x._1.count(_=> true) > 0)
                util.deleteRecursively(x._2)
              x._1
            })
            .runWith(Sink.seq)

          val res = Await.ready(future, Duration.Inf)
          future.value.get.get.flatten
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
