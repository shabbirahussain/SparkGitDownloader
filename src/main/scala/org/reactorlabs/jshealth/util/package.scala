package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * @author shabbirahussain
  */
package object util {
  case class ExecutionService[T](solvers: Iterator[() => T], nThreads: Int = 1)
    extends Iterator[T] {
    private case class Result(f: Future[Result], r: T)
    private val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nThreads))
    private val futureResults = mutable.Set[Future[Result]]()
    private def submitJob(s: () => T): Future[Result] = {
      lazy val fr: Future[Result] = Future{Result(fr, s())}(ec)
      futureResults += fr
      fr
    }
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {solvers.map(submitJob)}//.foreach(x=> x.foreach(_=>{}))

    override def hasNext(): Boolean = solvers.nonEmpty || futureResults.nonEmpty
    override def next(): T = {
      if (futureResults.isEmpty && solvers.hasNext)  submitJob(solvers.next())
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
