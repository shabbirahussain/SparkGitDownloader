package org.reactorlabs.jshealth

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe._
import scala.util.{Failure, Try}

import java.util.concurrent.TimeoutException

/**
  * @author shabbirahussain
  */
package object util {
  import scala.concurrent.ExecutionContext.Implicits.global

  case class TimedIterator[A](src : Iterator[A], timeout: Duration)
    extends Iterator[Try[A]] {
    private val fail = Failure(new TimeoutException("Iterator timed out after %s".format(timeout.toString)))
    private def fetchNext(): Try[A] = Try(Await.result(Future{src.next()}, timeout))

    private val limitTime = System.currentTimeMillis() + timeout.toMillis
    private var _next: Try[A] = fetchNext()

    def hasNext: Boolean = _next.isSuccess
    def next() : Try[A] = {
      val res = if (System.currentTimeMillis() > limitTime) fail else _next
      _next   = if (res.isSuccess) fetchNext() else res
      res
    }
  }

  case class ExecuteAfterLastElemIterator[A](src : Iterator[A], block: ()=> Unit)
    extends Iterator[A] {
    def hasNext: Boolean = {
      if (!src.hasNext) {
        block()
        false
      } else true
    }
    def next() : A = src.next()
  }

  case class ExecutionService[T](solvers: Iterable[() => T], ec: ExecutionContextExecutor)
    extends Iterator[T] {
    private case class Result(f: Future[Result], r: T)
    private val futureResults = mutable.Set[Future[Result]]()
    private def submitJob(s: () => T): Future[Result] = {
      lazy val fr: Future[Result] = Future{Result(fr, s())}(ec)
      futureResults += fr
      fr
    }
    solvers.map(submitJob)

    override def hasNext(): Boolean = futureResults.nonEmpty

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
