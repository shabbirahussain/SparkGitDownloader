package org.reactorlabs.jshealth

import java.nio.file.Paths

import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark}
import org.reactorlabs.jshealth.git.datastores.Keychain

package object git {
  private val gitRepoPath = prop.getProperty("git.repo.path")
  private val gitBaseUrl  = prop.getProperty("git.api.url")
  private val extensions  = prop.getProperty("git.download.extensions")
  private val keyFilePath = prop.getProperty("git.api.keys.path")

  private val gitRepoDir = Paths.get(gitRepoPath).toFile
  if (! gitRepoDir.exists()) gitRepoDir.mkdirs()
//  extensions.split(",").map(_.trim.toLowerCase).toSet

  private val gitHub   = new GitHub(new Keychain(keyFilePath))


  def exec:Unit ={
    gitHub.listFiles("shabbirahussain/SparkTest/master:src/")
  }
}
