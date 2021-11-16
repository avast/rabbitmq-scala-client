package com.avast.clients.rabbitmq.extras

import cats.effect.{Blocker, IO, Resource}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.execution.schedulers.CanBlock
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.Executors
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class TestBase extends FunSuite with MockitoSugar with Eventually with StrictLogging {
  protected implicit def taskToOps[A](t: Task[A]): TaskOps[A] = new TaskOps[A](t)
  protected implicit def resourceToIOOps[A](t: Resource[IO, A]): ResourceIOOps[A] = new ResourceIOOps[A](t)
  protected implicit def resourceToTaskOps[A](t: Resource[Task, A]): ResourceTaskOps[A] = new ResourceTaskOps[A](t)
}

object TestBase {
  val testBlockingScheduler: Scheduler = Scheduler.io(name = "test-blocking")
  val testBlocker: Blocker = Blocker.liftExecutorService(Executors.newCachedThreadPool())
}

class TaskOps[A](t: Task[A]) {
  def await(duration: Duration): A = t.runSyncUnsafe(duration)
  def await: A = await(10.seconds)
}

class ResourceIOOps[A](val r: Resource[IO, A]) extends AnyVal {
  def withResource[B](f: A => B): B = {
    withResource(f, Duration.Inf)
  }

  def withResource[B](f: A => B, timeout: Duration): B = {
    r.use(a => IO(f).map(_(a))).unsafeRunTimed(timeout).getOrElse(throw new TimeoutException("Timeout has occurred"))
  }
}

class ResourceTaskOps[A](val r: Resource[Task, A]) extends AnyVal {
  def withResource[B](f: A => B): B = {
    withResource(f, Duration.Inf)
  }

  def withResource[B](f: A => B, timeout: Duration): B = {
    r.use(a => Task.delay(f(a))).runSyncUnsafe(timeout)(TestBase.testBlockingScheduler, CanBlock.permit)
  }
}
