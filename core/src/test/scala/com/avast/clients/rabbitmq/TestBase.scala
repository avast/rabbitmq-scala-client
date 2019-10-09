package com.avast.clients.rabbitmq
import java.util.concurrent.Executors

import cats.effect.{Blocker, IO, Resource, SyncIO}
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

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.{higherKinds, implicitConversions}

@RunWith(classOf[JUnitRunner])
class TestBase extends FunSuite with MockitoSugar with Eventually with StrictLogging {
  protected implicit def taskToOps[A](t: Task[A]): TaskOps[A] = new TaskOps[A](t)
  protected implicit def resourceToSyncIOOps[A](t: Resource[SyncIO, A]): ResourceSyncIOOps[A] = new ResourceSyncIOOps[A](t)
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

class ResourceSyncIOOps[A](val r: Resource[SyncIO, A]) extends AnyVal {
  def withResource[B](f: A => B): B = {
    withResource(f, Duration.Inf)
  }

  def withResource[B](f: A => B, timeout: Duration): B = {
    r.use(a => SyncIO(f).map(_(a))).toIO.unsafeRunTimed(timeout).getOrElse(throw new TimeoutException("Timeout has occurred"))
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
