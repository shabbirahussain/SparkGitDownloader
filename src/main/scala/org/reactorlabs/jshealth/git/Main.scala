package org.reactorlabs.jshealth.git

import java.nio.file.Paths

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubRestV4, RepoManager}

/**
  * @author shabbirahussain
  */
object Main {
  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val gitHub: RepoManager  = new GitHubRestV4(prop.getProperty("git.api.keys.path"))
  private val crawlBatchSize = prop.getProperty("git.crawl.batch.size").toInt

  /**
    * Crawls the files from the frontier queue and stores results back to database.
    */
  def crawlFileHeads(): Unit = {
    def recursiveExplore(fht: FileHashTuple): Seq[FileHashTuple] = {
      if (fht.gitPath.isEmpty){
        val msg = "Processing: " + fht.owner + "/" + fht.repo + ":" + fht.branch
        logger.log(Level.INFO, msg)
        println(msg)
      }

      try{
        val children = gitHub.listFiles(fht.owner, fht.repo, fht.branch, fht.gitPath)
        val grandchildren = children
          .filter(_.fileType == FileTypes.tree)
          .flatMap(x=> recursiveExplore(x))

        grandchildren.++(children
          .filter(_.fileType == FileTypes.blob)
          .filter(x=> extensions.contains(x.gitPath.substring(x.gitPath.lastIndexOf(".") + 1))))
      } catch {
        case e: Exception => {
          Seq(FileHashTuple(
            owner     = fht.owner,
            repo      = fht.repo,
            branch    = fht.branch,
            gitPath   = fht.gitPath,
            fileType  = null,
            fileHash  = null,
            commitMsg = e.getMessage))
        }
      }
    }

    val (repos, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)

    val allFiles = repos.flatMap[FileHashTuple](x=> {
      recursiveExplore(
        FileHashTuple(owner = x._1,
          repo = x._2,
          branch = x._3))
    })

    dataStore.store(allFiles)
    val err = allFiles.filter(_.fileType == null)
      .map(x=> ((x.owner, x.repo, x.branch), 1))
      .reduceByKey(_+_)
      .map(x=> (FileHashTuple(x._1._1, x._1._2, x._1._3), x._2.toString))

    dataStore.markReposCompleted(token, err)
  }



  def main(args: Array[String]): Unit = {
    println("Git.Main")

//    crawlFileHeads()
    println(gitHub.listFiles("shabbirahussain", "SparkTest", "master", ""))
    println(gitHub.listFiles("shabbirahussain", "SparkTest", "master", ""))

    //    println(gitHub.getFileCommits("shabbirahussain/SparkTest/master:src/Main.scala"))
  }
}
