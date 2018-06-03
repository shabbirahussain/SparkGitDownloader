package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * @author shabbirahussain
  */
package object util {
  case class ExecutionService[T](solvers: Iterable[() => T], nThreads: Int = 1)
    extends Iterator[T] {
    private case class Result(f: Future[Result], r: T)
    private val ex = Executors.newFixedThreadPool(nThreads)
    private val ec = ExecutionContext.fromExecutor(ex)
    private val futureResults = mutable.Set[Future[Result]]()
    private def submitJob(s: () => T): Future[Result] = {
      lazy val fr: Future[Result] = Future{Result(fr, s())}(ec)
      futureResults += fr
      fr
    }
    solvers.map(submitJob)

    override def hasNext(): Boolean = {
      if (futureResults.isEmpty)
        ex.shutdown()
      futureResults.nonEmpty
    }
    override def next(): T = {
      val result = Await.result(Future.firstCompletedOf(futureResults.toSeq)(ec), Duration.Inf)
      futureResults -= result.f
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
