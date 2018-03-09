package com.avast.clients

import cats.arrow.FunctionK
import cats.syntax.either._
import cats.~>
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{MessageProperties, RabbitMQProducer}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try

package object rabbitmq {
  type FromTask[A[_]] = ~>[Task, A]

  implicit val fkTask: FunctionK[Task, Task] = new FunctionK[Task, Task] {
    override def apply[A](fa: Task[A]): Task[A] = fa
  }

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

  implicit val functorProducer: FunctorK[RabbitMQProducer] = new FunctorK[RabbitMQProducer] {
    override def mapK[F[_], G[_]](prod: RabbitMQProducer[F])(fk: ~>[F, G]): RabbitMQProducer[G] = new RabbitMQProducer[G] {
      override def send(routingKey: String, body: Bytes, properties: MessageProperties): G[Unit] = {
        fk(prod.send(routingKey, body, properties))
      }

      override def send(routingKey: String, body: Bytes): G[Unit] = {
        fk(prod.send(routingKey, body))
      }

      override def close(): Unit = prod.close()
    }
  }
}
