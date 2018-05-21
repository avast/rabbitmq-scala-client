package com.avast.clients

import cats.arrow.FunctionK
import cats.{~>, Monad}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.rabbitmq.client.{RecoverableChannel, RecoverableConnection}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.higherKinds
import scala.util.Try

package object rabbitmq {
  private[rabbitmq] type ServerConnection = RecoverableConnection
  private[rabbitmq] type ServerChannel = RecoverableChannel

  type DeliveryReadAction[F[_], -A] = Delivery[A] => F[DeliveryResult]
  type ParsingFailureAction[F[_]] = (String, Delivery[Bytes], ConversionException) => F[DeliveryResult]

  type FromTask[A[_]] = FunctionK[Task, A]
  type ToTask[A[_]] = FunctionK[A, Task]

  implicit val fkTask: FunctionK[Task, Task] = FunctionK.id

  implicit def fkToFuture(implicit ec: ExecutionContext): FromTask[Future] = new FunctionK[Task, Future] {
    override def apply[A](fa: Task[A]): Future[A] = fa.runAsync(Scheduler(ec))
  }

  implicit def fkToTry(implicit ec: ExecutionContext): FromTask[Try] = new FunctionK[Task, Try] {
    override def apply[A](task: Task[A]): Try[A] = Try {
      task.coeval(Scheduler(ec).withExecutionModel(ExecutionModel.SynchronousExecution))() match {
        case Right(a) => a
        case Left(fa) => Await.result(fa, Duration.Inf)
      }
    }
  }

  implicit val fkFromFuture: ToTask[Future] = new FunctionK[Future, Task] {
    override def apply[A](fa: Future[A]): Task[A] = Task.fromFuture(fa)
  }

  implicit val fkFromTry: ToTask[Try] = new FunctionK[Try, Task] {
    override def apply[A](fa: Try[A]): Task[A] = Task.fromTry(fa)
  }

  implicit def producerFunctorK[A]: FunctorK[RabbitMQProducer[?[_], A]] = new FunctorK[RabbitMQProducer[?[_], A]] {
    override def mapK[F[_], G[_]](af: RabbitMQProducer[F, A])(fToG: ~>[F, G]): RabbitMQProducer[G, A] =
      (routingKey: String, body: A, properties: Option[MessageProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  /*
   * This is needed because last version of Monix depends on older cats than we have. It does not cause problems in general but we're unable
   * to use `monix-cats` extension since it's missing some trait removed from Cats.
   */
  private[rabbitmq] implicit val taskMonad: Monad[Task] = new Monad[Task] {
    override def pure[A](x: A): Task[A] = Task.now(x)

    override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = Task.tailRecM(a)(f)
  }

}
