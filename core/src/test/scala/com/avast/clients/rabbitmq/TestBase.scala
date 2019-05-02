package com.avast.clients.rabbitmq
import cats.effect.Resource
import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler.Implicits.global
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.duration._
import scala.language.{higherKinds, implicitConversions}

@RunWith(classOf[JUnitRunner])
class TestBase extends FunSuite with MockitoSugar with Eventually with StrictLogging {
  protected implicit def taskToOps[A](t: Task[A]): TaskOps[A] = new TaskOps[A](t)
  protected implicit def resourceToOps[A](t: Resource[Coeval, A]): ResourceCoevalOps[A] = new ResourceCoevalOps[A](t)
  protected implicit def resourceToOps[A](t: Resource[Task, A]): ResourceTaskOps[A] = new ResourceTaskOps[A](t)
}

class TaskOps[A](t: Task[A]) {
  def await(duration: Duration): A = t.runSyncUnsafe(duration)
  def await: A = await(10.seconds)
}

class ResourceCoevalOps[A](t: Resource[Coeval, A]) {
  def unwrap: A = t.allocated.value()._1
}

class ResourceTaskOps[A](t: Resource[Task, A]) {
  def await(duration: Duration): A = t.allocated.runSyncUnsafe(duration)._1
  def await: A = await(10.seconds)
}
