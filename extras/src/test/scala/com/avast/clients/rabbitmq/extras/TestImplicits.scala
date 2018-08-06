package com.avast.clients.rabbitmq.extras

import cats.arrow.FunctionK
import com.avast.clients.rabbitmq.{FromTask, ToTask}
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

object TestImplicits {
  def fkTaskToTry(timeout: Duration)(implicit ec: ExecutionContext): FromTask[Try] = new FunctionK[Task, Try] {
    override def apply[A](task: Task[A]): Try[A] = Try {
      task.coeval(Scheduler(ec).withExecutionModel(ExecutionModel.SynchronousExecution))() match {
        case Right(a) => a
        case Left(fa) => Await.result(fa, timeout)
      }
    }
  }

  implicit def fkTaskToTry(implicit ec: ExecutionContext): FromTask[Try] = fkTaskToTry(Duration.Inf)

  implicit val fkTaskFromTry: ToTask[Try] = new FunctionK[Try, Task] {
    override def apply[A](fa: Try[A]): Task[A] = Task.fromTry(fa)
  }
}
