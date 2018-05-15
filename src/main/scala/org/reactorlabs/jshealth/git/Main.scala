package org.reactorlabs.jshealth.git

import java.io.File
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.io.Files
import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util
import org.reactorlabs.jshealth.util._
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.reactorlabs.jshealth.models.{FileHashTuple, Schemas}
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

  private val fhtExtensions  = prop.getProperty("git.download.fht.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val conExtensions  = prop.getProperty("git.download.con.extensions").toLowerCase.split(",").map(_.trim).toSet
  private val keychain        = new Keychain(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize  = prop.getProperty("git.crawl.batch.size").toInt
  private val numCores        = prop.getProperty("git.downloader.numworkers").toInt
  private val groupSize       = prop.getProperty("git.downloader.group.size").toInt
  private val consolFreq      = prop.getProperty("git.downloader.consolidation.frequency").toInt
  private val gitPath         = {
    var path = prop.getProperty("git.repo.path")
    if (path == null || path.isEmpty)
      path = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp") + "/git/"
    new File(path)
  }
  private val existingHashMap = TrieMap[String, Unit]()
  private val gitHub : RepoManager = new GitHubClient(
      keychain   = keychain,
      workingGitDir = gitPath.toPath.toAbsolutePath.toString)
  private val genDataFor = {
    prop.getProperty("ds.filestore.generate.data.for")
      .toLowerCase.split(",").map(_.trim)
      .toSet
      .map(Schemas.withName)
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

    logger.log(Level.INFO, "\tPerforming Setup ...")

    val rawData = links
      .repartition(crawlBatchSize / groupSize)
      .mapPartitions(_.grouped(groupSize), preservesPartitioning = true)
      .map(x=> {fs.delete(new Path(gitPath.toURI), true);x}) // Cleanup previous clones
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
      .flatMap(_.filter(_.isDefined).map(_.get)) // Remove unsuccessful clones
      .flatMap(git=> {
          val repo = git.getRepository.getDirectory.getParentFile
          val allCommits = Try(gitHub.getRepoFilesHistory(git))
          match {
            case _@Failure(e) =>
              // Log all errors to the database
              logger.log(Level.ERROR, e.getMessage)
              dataStore.markRepoError(owner = repo.getParentFile.getName, repo = repo.getName, branch = "master", err = e.getMessage)
              Seq.empty
            case success@_ => success.get
          }

          val commits = if (fhtExtensions.isEmpty) allCommits
          else allCommits.filter(fht=> fhtExtensions.contains(Files.getFileExtension(fht.gitPath)))

          val contents = commits
            .filter(_.fileHash != null)
            .filter(fht => conExtensions.contains(Files.getFileExtension(fht.gitPath)))
            .filter(fht => !existingHashMap.contains(fht.fileHash))
            .map(fht => {existingHashMap.+(fht.fileHash -> Unit); fht})
            .map(fht => Try((fht.fileHash, new String(gitHub.getFileContents(git, fht.fileHash),"UTF-8"))))
            .filter(_.isSuccess).map(_.get)
            .map(x => (Schemas.CONTENTS, (x._1, escapeString(x._2))))

          val commitMsg = commits
            .map(x => (Schemas.COMMIT_MESSAGES, (x.commitId, x.author, escapeString(x.longMsg))))
            .distinct

          val fht = commits
            .map(x => (Schemas.FILE_METADATA, (x.owner, x.repo, x.gitPath, x.fileHash, x.commitTime, x.commitId)))

          var ret: Seq[(Schemas.Value, Any)] = Seq.empty
          if(genDataFor.contains(Schemas.FILE_METADATA))   ret = ret.union(fht)
          if(genDataFor.contains(Schemas.CONTENTS))        ret = ret.union(contents)
          if(genDataFor.contains(Schemas.COMMIT_MESSAGES)) ret = ret.union(commitMsg)
          ret
        }) // Explode data into splits
      .persist()
    rawData.checkpoint()

    if(genDataFor.contains(Schemas.FILE_METADATA)){
      val data = rawData
        .filter(_._1 == Schemas.FILE_METADATA)
        .map(_._2.asInstanceOf[(String, String, String, String, Long, String)])
        .toDF(Schemas.asMap(Schemas.FILE_METADATA)._3:_*)
      dataStore.store(data, folder = "%s/%s".format(token.toString, Schemas.FILE_METADATA))
    }
    if(genDataFor.contains(Schemas.COMMIT_MESSAGES)){
      val data = rawData
        .filter(_._1 == Schemas.COMMIT_MESSAGES)
        .map(_._2.asInstanceOf[(String, String, String)])
        .toDF(Schemas.asMap(Schemas.COMMIT_MESSAGES)._3:_*)
      dataStore.store(data, folder = "%s/%s".format(token.toString, Schemas.COMMIT_MESSAGES))
    }
    if(genDataFor.contains(Schemas.CONTENTS)){
      val data = rawData
        .filter(_._1 == Schemas.CONTENTS)
        .map(_._2.asInstanceOf[(String, String)])
        .toDF(Schemas.asMap(Schemas.CONTENTS)._3:_*)
        .dropDuplicates(Schemas.asMap(Schemas.CONTENTS)._2)
      dataStore.store(data, folder = "%s/%s".format(token.toString, Schemas.CONTENTS))

      val data2 = rawData
        .filter(_._1 == Schemas.CONTENTS)
        .map(_._2.asInstanceOf[(String, String)]._1)
        .toDF(Schemas.asMap(Schemas.INDEX)._3:_*)
        .dropDuplicates(Schemas.asMap(Schemas.INDEX)._2)
      dataStore.store(data2, folder = "%s/%s".format(token.toString, Schemas.INDEX))
    }

    dataStore.markRepoCompleted(links.map(x=> (x._1, x._2, x._3)))
    links.unpersist(blocking = false)
    rawData.unpersist(blocking = true)
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
