package org.reactorlabs.jshealth.git

import java.io.File
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main._
import org.reactorlabs.jshealth.datastores.Keychain
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}
import org.reactorlabs.jshealth.util._
import org.apache.spark.storage.StorageLevel
import org.eclipse.jgit.api.errors.{InvalidRemoteException, TransportException}
import org.eclipse.jgit.errors.NoRemoteRepositoryException
import org.reactorlabs.jshealth.models.{FileHashTuple, Schemas}
import org.reactorlabs.jshealth.util.escapeString
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.eclipse.jgit.api.Git
import monix.tail._
import monix.eval._
import monix.execution.CancelableFuture
import monix.reactive.{Consumer, Observable}

import scala.collection.JavaConversions._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.duration._
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
  private val nWorkers          = prop.getProperty("git.downloader.numworkers").toInt
  private val consolFreq      = prop.getProperty("git.downloader.consolidation.frequency").toInt
  private val genDataFor      = prop.getProperty("git.generate.data.for")
    .toUpperCase.split(",").map(_.trim)
    .toSet
    .map(Schemas.withName)
  private val gitPath    = {
    val hadoopDir = sc.hadoopConfiguration.get("hadoop.tmp.dir", "/tmp")
    new File( "%s/repos/%d".format(hadoopDir, System.currentTimeMillis()))
  }
  private val gitClient : RepoManager = new GitHubClient(
      keychain   = keychain,
      workingGitDir = gitPath.toPath.toAbsolutePath.toString
  )

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
    * @param branch is the branch to clone.
    * @param token is the token id of the process.
    * @return A single element seq of Git object if successful.
    */
  private def tryCloneRepo(owner: String, repo: String, branch: String, token: Long)
  : Seq[Git] = {
    Try(gitClient.gitCloneRepo(owner, repo))
    match {
      case _ @ Failure(e) =>
        logger.log(Level.ERROR, "Cloning Error [https://github.com/%s/%s]".format(owner, repo) + e.getMessage)
        ds.markRepoError(owner, repo, branch, err = e.getMessage, token = token)
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
        logger.log(Level.ERROR, e.getMessage)
        ds.markRepoError(owner, repo, branch = "master", err = e.getMessage, token = token)
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
        logger.log(Level.ERROR, e.getMessage)
        ds.markRepoError(owner, repo, branch = "master", err = e.getMessage, token = token)
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
    logger.log(Level.INFO, "Checking out links ...")
    val (links, token) = ds.checkoutReposToCrawl(crawlBatchSize)
    links.persist(StorageLevel.DISK_ONLY)
    if (links.isEmpty()) return false

    logger.log(Level.INFO, "Initializing ...")
    val data = links
      .repartition(crawlBatchSize / groupSize)
      .mapPartitions(_.grouped(groupSize), preservesPartitioning = true)
      .filter(_=> {
        fs.delete(new Path(gitPath.toURI), true)
        true
      })  // Cleanup previous clones
      .filter(_=> {
        logger.log(Level.INFO,  "Loading next batch ...")
        true
      })  // Progress monitor stage
      .flatMap(batch=> {
        import monix.execution.Scheduler.Implicits.global
        val res = Observable
          .fromIterable(batch)
          .mapParallelUnordered(nWorkers * 2)(x=> Task(tryCloneRepo(x._1, x._2, x._3, token)))
          .filter(_.nonEmpty).map(_.head)
          .mapParallelUnordered(nWorkers)(x=> Task(tryGetRepoFilesHistory(x, token)))
          .toListL.runAsync
        Await.result(res, Duration.Inf)
      })
      .flatMap(x=> x)
      .filter(fht=> metaExtns.isEmpty || metaExtns.contains(scala.reflect.io.File(fht.gitPath).extension))
      .map(x=> (x.owner, x.repo, x.gitPath, x.fileHash, x.commitTime, x.commitId))
      .toDF(Schemas.asMap(Schemas.FILE_METADATA)._3:_*)
    ds.storeRecords(data, folder = "%d/%s".format(token, Schemas.FILE_METADATA))

    ds.markRepoCompleted(links.map(x=> (x._1, x._2, x._3)), token)
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

//    logger.log(Level.INFO, "\tChecking out links ...")
//    val (links, token) = ds.checkoutReposToCrawl(crawlBatchSize)
//    links.persist(StorageLevel.DISK_ONLY)
//    if (links.isEmpty()) return false
//
//    logger.log(Level.INFO, "\tInitializing ...")
//    val rawData: RDD[(Schemas.Value, Any)] = links
//      .repartition(crawlBatchSize / groupSize)
//      .mapPartitions(_.grouped(groupSize), preservesPartitioning = true)
//      .filter(_=> {
//        fs.delete(new Path(gitPath.toURI), true)
//        true
//      })  // Cleanup previous clones
//      .filter(_=> {
//        val msg = "\n" + new Date() + "\tLoading next batch ..."
//        println(msg)
//        logger.log(Level.INFO, msg)
//        true
//      })  // Progress monitor map stage
//      .flatMap(_
//        .flatMap(x=> tryCloneRepo(x._1, x._2, x._3, token)) // Clone the git repo batch
//        .flatMap(git=> {
//          val commits = tryGetRepoFilesHistory(git, token)
//            .filter(fht=> metaExtns.isEmpty || metaExtns.contains(File(fht.gitPath).extension))
//
//          val fileMetadata = commits
//            .map(x=> (Schemas.FILE_METADATA, (x.owner, x.repo, x.gitPath, x.fileHash, x.commitTime, x.commitId)))
//
//          val contents = commits
//            .filter(_.fileHash != null)
//            .filter(fht=> contentsExtn.contains(File(fht.gitPath).extension))
//            .filter(fht=> !downloaded.contains(fht.fileHash))
//            .filter(fht=> {
//              downloaded.+(fht.fileHash -> Unit)
//              true
//            }) // Update local hash map of downloaded contents
//            .flatMap(fht=> tryGetFileContents(fht.fileHash, git, token))
//            .map(x=> (Schemas.CONTENTS, (x._1, escapeString(x._2))))
//
//          val commitMsg = commits
//            .map(x=> (Schemas.COMMIT_MESSAGES, (x.commitId, x.author, escapeString(x.longMsg))))
//            .distinct
//
//          var ret: Seq[(Schemas.Value, Any)] = Seq.empty
//          if(genDataFor.contains(Schemas.FILE_METADATA))   ret = ret.union(fileMetadata)
//          if(genDataFor.contains(Schemas.CONTENTS))        ret = ret.union(contents)
//          if(genDataFor.contains(Schemas.COMMIT_MESSAGES)) ret = ret.union(commitMsg)
//          ret
//        })
//      .toIterator) // Explode data into splits
//      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
////    if(genDataFor.contains(Schemas.FILE_METADATA)){
////      val data = rawData
////        .filter(_._1 == Schemas.FILE_METADATA)
////        .map(_._2.asInstanceOf[(String, String, String, String, Long, String)])
////        .toDF(Schemas.asMap(Schemas.FILE_METADATA)._3:_*)
////      ds.storeRecords(data, folder = "%s/%s".format(token.toString, Schemas.FILE_METADATA))
////    }
////    if(genDataFor.contains(Schemas.COMMIT_MESSAGES)){
////      val data = rawData
////        .filter(_._1 == Schemas.COMMIT_MESSAGES)
////        .map(_._2.asInstanceOf[(String, String, String)])
////        .toDF(Schemas.asMap(Schemas.COMMIT_MESSAGES)._3:_*)
////      ds.storeRecords(data, folder = "%s/%s".format(token.toString, Schemas.COMMIT_MESSAGES))
////    }
////    if(genDataFor.contains(Schemas.CONTENTS)){
////      val data = rawData
////        .filter(_._1 == Schemas.CONTENTS)
////        .map(_._2.asInstanceOf[(String, String)])
////        .toDF(Schemas.asMap(Schemas.CONTENTS)._3:_*)
////        .dropDuplicates(Schemas.asMap(Schemas.CONTENTS)._2)
////      ds.storeRecords(data, folder = "%s/%s".format(token.toString, Schemas.CONTENTS))
////
////      val data2 = rawData
////        .filter(_._1 == Schemas.CONTENTS)
////        .map(_._2.asInstanceOf[(String, String)]._1)
////        .toDF(Schemas.asMap(Schemas.INDEX)._3:_*)
////        .dropDuplicates(Schemas.asMap(Schemas.INDEX)._2)
////      ds.storeRecords(data2, folder = "%s/%s".format(token.toString, Schemas.INDEX))
////    }
//
//    ds.markRepoCompleted(links.map(x=> (x._1, x._2, x._3)), token)
//    links.unpersist(blocking = false)
//    rawData.unpersist(blocking = true)
    true
  }

  def main(args: Array[String])
  : Unit = {
    logger.log(Level.INFO, "Git.Main")
    logger.log(Level.DEBUG, "Cloning repos in [%s]".format(gitPath.toPath.toAbsolutePath))

    var continue = false
    var ctr: Long = 0
    do{
      continue = crawlFileHistory()
      ctr += 1
      if (ctr % consolFreq == 0){
        logger.log(Level.INFO, "\tConsolidating ...")
        ds.consolidateData(genDataFor)
      }
    } while(continue)
  }
}
