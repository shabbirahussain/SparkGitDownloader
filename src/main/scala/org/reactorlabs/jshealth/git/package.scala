package org.reactorlabs.jshealth

import java.nio.file.Paths

import org.reactorlabs.jshealth.Main.{logger, prop, sc, spark}

package object git {
  private val gitRepoPath = prop.getProperty("git.repo.path")
  private val gitBaseUrl  = prop.getProperty("git.api.url")
  private val extensions  = prop.getProperty("git.download.extensions")

  private val gitRepoDir = Paths.get(gitRepoPath).toFile
  if (! gitRepoDir.exists()) gitRepoDir.mkdirs()

  private val git = new GitHubWrap(extensions.split(",").map(_.trim.toLowerCase).toSet, gitRepoPath, gitBaseUrl)
}
