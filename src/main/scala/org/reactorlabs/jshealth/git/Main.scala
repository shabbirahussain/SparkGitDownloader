package org.reactorlabs.jshealth.git

import java.util.Date

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubClient, RepoManager}

/**
  * @author shabbirahussain
  */
object Main {
  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val apiKeysPath = prop.getProperty("git.api.keys.path")
  private val gitPath     = prop.getProperty("git.repo.path") //Files.createTempDir()
  private val gitHub: RepoManager  = new GitHubClient(extensions = extensions, gitPath)
  private val crawlBatchSize = prop.getProperty("git.crawl.batch.size").toInt
  private val lineWidth      = prop.getProperty("console.line.width").toInt


  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHistory()
  : Boolean = {
    val (links, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)
    val cnt = links.count()
    if (cnt == 0) return false

    links
        .foreach(x=> {
          val msg = "Processing: " + x._1 + "/" + x._2 + "/" + x._3
          println((new Date()) + msg)
          logger.log(Level.INFO, msg)

          val files = gitHub.getFileCommits(x._1, x._2, x._3)

          dataStore.storeHistory(files)
          dataStore.markRepoCompleted(FileHashTuple(owner = x._1, repo = x._2, branch = x._3))
        })
    true
  }

  def main(args: Array[String])
  : Unit = {
    println("Git.Main")

//    var continue = true
//    do{
//      continue = crawlFileHistory()
//    } while(continue)

    gitHub.getFileCommits("shabbirahussain", "SparkTest", "master")
  }
}
