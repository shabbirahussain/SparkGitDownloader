package org.reactorlabs.jshealth.git

import java.nio.file.Paths
import java.util.Date

import org.apache.log4j.Level
import org.reactorlabs.jshealth.Main.{dataStore, logger, prop, sc, spark}
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubApiV4, RepoManager}

/**
  * @author shabbirahussain
  */
object Main {
  private val extensions  = prop.getProperty("git.download.extensions")
    .toLowerCase.split(",").map(_.trim).toSet
  private val apiKeysPath = prop.getProperty("git.api.keys.path")
  private val gitHub: RepoManager  = new GitHubApiV4(apiKeysPath)
  private val crawlBatchSize = prop.getProperty("git.crawl.batch.size").toInt
  private val lineWidth      = prop.getProperty("console.line.width").toInt

  /** Crawls the files from the frontier queue and stores files list back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHeads()
  : Boolean = {
    gitHub.reloadAPIKeys()

    val (repos, token) = dataStore.checkoutReposToCrawl(crawlBatchSize)
    if  (repos.count() == 0) return false

    repos
      .map[(FileHashTuple, Seq[FileHashTuple])](x=> {
        val fht = FileHashTuple(owner = x._1, repo = x._2, branch = x._3)
        (fht, recursiveExplore(fht))
      })
      .foreach(x=> {
        dataStore.store(x._2)
        dataStore.markRepoCompleted(x._1)
      })

    def recursiveExplore(fht: FileHashTuple): Seq[FileHashTuple] = {
      if (fht.gitPath.isEmpty){
        val msg = "Processing: " + fht.owner + "/" + fht.repo + ":" + fht.branch
        logger.log(Level.INFO, msg)
      } else {
        val msg = "Processing: " + fht.owner + "/" + fht.repo + "/" + fht.branch + ":" + fht.gitPath
        print("\r" + msg + (" " * math.max(0, lineWidth - msg.length)))
      }
      print("\r")

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
    true
  }

  /** Crawls the files from the frontier queue and stores commit history back to database.
    *
    * @return status if current sprint had any files to work on.
    */
  def crawlFileHistory()
  : Boolean = {
    val (links, token) = dataStore.checkoutLinksToCrawl(crawlBatchSize * 5)
    val cnt = links.count()
    println((new Date()) + " Processing: " + cnt + " links.")

    if (cnt == 0) return false
    val allFiles = links.flatMap[FileHashTuple](x=> gitHub.getFileCommits(x._1, x._2, x._3, x._4))

    dataStore.storeHistory(allFiles)

    val err = allFiles.filter(_.fileType == null)
      .map(x=> ((x.owner, x.repo, x.branch), 1))
      .reduceByKey(_+_)
      .map(x=> (FileHashTuple(x._1._1, x._1._2, x._1._3), x._2.toString))

    dataStore.markLinksAsCompleted(token, err)
    true
  }

  def main(args: Array[String])
  : Unit = {
    println("Git.Main")
//    crawlFileHeads()

    var continue = true
    do{
//      continue = crawlFileHeads()
      continue = crawlFileHistory()
    } while(continue)

//    println(gitHub.listFiles("avelinoferreiragf", "avelino", "master", "Gestao Estrategica TI/02 - Alinhamento Estratégico de TI"))
//    println(gitHub.listFiles("shabbirahussain", "SparkTest", "master", ""))
//    println(gitHub.getFileCommits("shabbirahussain/SparkTest/master:src/Main.scala"))
  }
}
