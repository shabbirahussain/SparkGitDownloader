package org.reactorlabs.git.downloader

import java.nio.file.Paths

import org.reactorlabs.git.models.Extensions

/** Class responsible for downloading files from github.com
  *
  * @param extensions is the set of extensions.
  * @param repoPath is the base location of the repository.
  *
  * @author shabbir.ahussain
  */
@SerialVersionUID(100L)
class GitDownloader(extensions  : Set[Extensions.Value],
                    repoPath: String) {
  private val repoDir = Paths.get(repoPath).toFile
  if (! repoDir.exists()) repoDir.mkdirs()


  def getProject(url: String): Unit = {


  }

  private def fetchProject(url: String): Unit = {

  }
}
