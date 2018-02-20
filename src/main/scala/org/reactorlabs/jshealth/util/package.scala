package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths

/**
  * @author shabbirahussain
  */
package object util {
  def deleteRecursively(file: File)
  : Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete){
      //throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }

  def recursiveListFiles(f: File)
  : Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def main(args: Array[String]): Unit = {
    deleteRecursively(Paths.get("/Users/shabbirhussain/Data/project/mysql-2018-02-01/git/aaronksaunders/TiMongoDBJS/build/iphone/build/Debug-iphoneos/question.app").toFile)
  }
}
