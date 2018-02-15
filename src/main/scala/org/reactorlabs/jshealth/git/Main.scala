package org.reactorlabs.jshealth.git

import java.nio.file.Paths

import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark}
import org.reactorlabs.jshealth.datastores.{DataStore, LocalStore}
import org.reactorlabs.jshealth.models.{FileHashTuple, FileTypes}
import org.reactorlabs.jshealth.repomanagers.{GitHubRestV4, RepoManager}

object Main {
  private val gitRepoPath = prop.getProperty("git.repo.path")
  private val extensions  = prop.getProperty("git.download.extensions")
    .split(",").map(_.trim.toLowerCase).toSet
  private val keyFilePath = prop.getProperty("git.api.keys.path")

  private val gitRepoDir = Paths.get(gitRepoPath).toFile
  if (! gitRepoDir.exists()) gitRepoDir.mkdirs()

  private val gitHub: RepoManager  = new GitHubRestV4(keyFilePath)
  private val dataStore: DataStore = new LocalStore()


  def crawlFileHeads(): Unit = {
    def recursiveExplore(url: String): Seq[FileHashTuple] = {
      try{
        val children = gitHub.listFiles(url)
        val grandchildren = children
          .filter(_.fileType == FileTypes.tree)
          .flatMap(x=> recursiveExplore(x.url))

        grandchildren ++ children.filter(_.fileType == FileTypes.blob)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          Seq(FileHashTuple(url = url,
            fileType = null,
            fileHash = null,
            commitMsg = e.getMessage))
        }
      }
    }

    val (repos, token) = dataStore.checkoutReposToCrawl(1)
    val allFiles = repos.flatMap(recursiveExplore)
    dataStore.store(allFiles)
    val err = allFiles.filter(_.fileType == null)
      .map(x=> (x.url, x.commitMsg))

    dataStore.markReposCompleted(token, err)
  }



  def main(args: Array[String]): Unit = {
    crawlFileHeads()
    //    println(gitHub.listFiles("shabbirahussain/SparkTest/master:src"))

    //    println(gitHub.getFileCommits("shabbirahussain/SparkTest/master:src/Main.scala"))
  }
}
