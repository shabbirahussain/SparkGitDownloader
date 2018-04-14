package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * @author shabbirahussain
  */
package object util {
  class ExecutionQueue[T](solvers: Iterable[() => T], nThreads: Int = 1) extends Iterator[T]{
    private case class Result(f: Future[Result], r: T)
    private val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nThreads))
    private var futures: Set[Future[Result]] = solvers.map(s=> {
      lazy val f: Future[Result]  = Future{Result(f, s())}(ec)
      f
    }).toSet
    def hasNext(): Boolean = futures.nonEmpty
    def next(): T = {
      val result = Await.result(Future.firstCompletedOf(futures.toSeq)(ec), Duration.Inf)
      futures -= result.f
      result.r
    }
  }

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
}
