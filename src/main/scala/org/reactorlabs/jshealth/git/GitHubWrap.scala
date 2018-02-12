package org.reactorlabs.jshealth.git

import java.nio.file.Paths

/** Class responsible for downloading files from github.com. It serves as a wrapper over Git API
  *
  * @param extensions is the set of extensions.
  *
  * @author shabbir.ahussain
  */
@SerialVersionUID(100L)
class GitHubWrap(extensions: Set[String],
                 gitRepoPath: String,
                 gitBaseUrl: String) {



  def getProject(url: String): Unit = {


  }

//  def listFiles(url: String): Seq[String] = {
//
//  }

  private def fetchProject(url: String): Unit = {

  }
}
