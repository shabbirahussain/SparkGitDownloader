package org.reactorlabs.jshealth.git

import java.io.File
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util
import org.reactorlabs.jshealth.util._
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.reactorlabs.jshealth.models.FileHashTuple
import org.reactorlabs.jshealth.util.escapeString
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Random, Success, Try}
/**
  * @author shabbirahussain
  */
object Main {
  import sqlContext.implicits._

  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize  = prop.getProperty("git.crawl.batch.size").toInt
  private val numCores        = prop.getProperty("git.downloader.numworkers").toInt
  private val groupSize       = prop.getProperty("git.downloader.group.size").toInt
  private val consolFreq      = prop.getProperty("git.downloader.consolidation.frequency").toInt
  private val gitPath         = {
    var path = prop.getProperty("git.repo.path")
    if (path.isEmpty) path = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp")
    new File(path)
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
    logger.log(Level.INFO, "\tChecking out links ...")
    val (links, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)
    links.persist(StorageLevel.DISK_ONLY)
    if (links.isEmpty()) return false
    logger.log(Level.INFO, "\tPerforming Cleanup ...")

    val gitHub = getNewGitHubClient()

    val fht = links
      .repartition(crawlBatchSize / groupSize)
      .mapPartitions(x=> {
        fs.delete(new Path(gitPath.toURI), true); x.grouped(groupSize)
      }, preservesPartitioning = true)
      .map(x=> {
        val msg = "\n" + (new Date()) + "\tLoading next batch ..."
        println(msg)
        logger.log(Level.INFO, msg)
        x
      }) // Progress monitor map stage
      .map(y=> new ExecutionQueue(y.map(x=> ()=> {
            Try(gitHub.gitCloneRepo(x._1, x._2))
            match {
              case _ @ Failure(e) =>
                // Log all errors to the database
                logger.log(Level.ERROR, "Cloning Error [https://github.com/%s/%s]".format(x._1, x._2) + e.getMessage)
                dataStore.markRepoError(owner = x._1, repo = x._2, branch = x._3, err = e.getMessage)
                e match {
                  case _: TransportException | _: InvalidRemoteException =>
                  case e: Throwable => e.printStackTrace()
                }
                None
              case success @ _ => Some(success.get)
            }
          }), nThreads = numCores)) // Clone the git repo batch
      .map(_.filter(_.isDefined).map(_.get)) // Remove unsuccessful clones
      .map(y=> new ExecutionQueue(y.map(git=> ()=>{
            val repo = git.getRepository.getDirectory.getParentFile
            val commits = Try(gitHub.getRepoFilesHistory(git))
            match {
              case _@Failure(e) =>
                // Log all errors to the database
                logger.log(Level.ERROR, e.getMessage)
                dataStore.markRepoError(owner = repo.getParentFile.getName, repo = repo.getName, branch = "master", err = e.getMessage)
                Seq.empty
              case success@_ => success.get
            }

            val res = commits
              .map(x =>
                ("commitMsg", """%s""".format(x.commitId), """%s,%s""".format(x.author, escapeString(x.longMsg)))) // Commit Messages
              .distinct
              .union(commits.map(x =>
                ("indexes", x.fileHash, ""))) // Indexes
              .union(commits.filter(_.contents.isDefined).map(x =>
                ("contents", x.fileHash, """%s""".format(escapeString(x.contents.get))))) // Contents
              .union(commits.map(x =>
                ("fht"
                  , """%s,%s,%s,%s,%d""".format(x.owner
                    , x.repo
                    , x.gitPath
                    , x.fileHash
                    , x.commitTime)
                  , """%s""".format(x.commitId))
              )) // FileHashTuple

            if (commits.count(_=>true) >=0) fs.delete(new Path(repo.toURI), true)
            res
        }).toIterable, nThreads = numCores)) // Explode data into categories
      .flatMap(_.flatten)
      .toDF("SPLIT", "TRUE_KEY", "VALUE")
      .select($"SPLIT",
        concat($"TRUE_KEY", when($"VALUE" === "", "").otherwise(","), $"VALUE"))

    dataStore.storeHistory(fht, token.toString)
    dataStore.markRepoCompleted(links.map(x=> (x._1, x._2, x._3)))

    links.unpersist(blocking = false)
    true
  }

  def main(args: Array[String])
  : Unit = {
    println("Git.Main")

    var continue = false
    var ctr = 0
    do{
      continue = crawlFileHistory()
      ctr += 1
      if (ctr % consolFreq == 0){
        val msg = "\tConsolidating ..."
        logger.log(Level.INFO, msg)
        dataStore.consolidateData()
      }
    } while(continue)
  }
}
