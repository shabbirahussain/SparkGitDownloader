package org.reactorlabs.jshealth

import java.io.File

/**
  * @author shabbir.ahussain
  */
package object util {
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
}
