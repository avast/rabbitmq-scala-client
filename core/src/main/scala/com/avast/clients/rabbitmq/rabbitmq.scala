package com.avast.clients

import cats.arrow.FunctionK
import cats.syntax.either._
import cats.~>
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try

package object rabbitmq {
  type FromTask[A[_]] = ~>[Task, A]

  implicit val fkTask: FunctionK[Task, Task] = FunctionK.id

  implicit def fkFuture(implicit ec: ExecutionContext): FunctionK[Task, Future] = new FunctionK[Task, Future] {
    override def apply[A](fa: Task[A]): Future[A] = fa.runAsync(Scheduler(ec))
  }

  implicit val fkTry: FunctionK[Task, Try] = new FunctionK[Task, Try] {
    override def apply[A](fa: Task[A]): Try[A] = {
      fa.coeval(Scheduler.Implicits.global.withExecutionModel(ExecutionModel.SynchronousExecution))()
        .leftMap(_ => new RuntimeException("This task should be executed synchronously"))
        .toTry
    }
  }

}
