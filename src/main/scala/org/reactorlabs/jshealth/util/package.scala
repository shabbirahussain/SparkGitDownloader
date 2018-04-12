package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * @author shabbirahussain
  */
package object util {
  def deleteRecursively(file: File)
  : Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    Try(file.delete)
  }

  def recursiveListFiles(f: File)
  : Array[File] = {
    val these = f.listFiles
      .filter(!_.isHidden)

    these
      .filter(_.isFile)
      .++(these
        .filter(_.isDirectory)
        .flatMap(recursiveListFiles)
      )
  }

  def escapeString(raw: String): String = Literal(Constant(raw)).toString

  def main(args: Array[String]): Unit = {
    deleteRecursively(Paths.get("/Users/shabbirhussain/Data/project/mysql-2018-02-01/git/aaronksaunders/TiMongoDBJS/build/iphone/build/Debug-iphoneos/question.app").toFile)
  }
}
