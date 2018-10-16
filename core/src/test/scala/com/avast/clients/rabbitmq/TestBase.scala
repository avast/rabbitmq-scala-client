package com.avast.clients.rabbitmq
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class TestBase extends FunSuite with MockitoSugar with Eventually with StrictLogging {
  protected implicit def taskToOps[A](t: Task[A]): TaskOps[A] = new TaskOps[A](t)
}

class TaskOps[A](t: Task[A]) {
  def await(duration: Duration): A = t.runSyncUnsafe(duration)
  def await: A = await(Duration.Inf)
}
