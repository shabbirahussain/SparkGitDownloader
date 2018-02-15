package org.reactorlabs.jshealth.git

import java.nio.file.Paths

import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark}
import org.reactorlabs.jshealth.datastores.{DataStore, LocalStore}
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


  def getFileHeads(): Unit = {
    val (repos, token) = dataStore.checkoutReposToCrawl(2)

//    repos.map()

//    println(gitHub.listFiles("shabbirahussain/SparkTest/master:src"))

//    println(gitHub.getFileCommits("shabbirahussain/SparkTest/master:src/Main.scala"))
  }



  def main(args: Array[String]): Unit = {
    getFileHeads()
  }
}
