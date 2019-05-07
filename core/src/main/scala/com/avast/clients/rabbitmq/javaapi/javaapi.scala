package com.avast.clients.rabbitmq

import cats.arrow.FunctionK
import cats.~>
import com.avast.clients.rabbitmq.api.{
  Delivery => ScalaDelivery,
  DeliveryResult => ScalaDeliveryResult,
  DeliveryWithHandle => ScalaDeliveryWithHandle,
  MessageProperties => ScalaProperties,
  PullResult => ScalaPullResult,
  RabbitMQProducer => ScalaProducer,
  RabbitMQPullConsumer => ScalaPullConsumer
}
import mainecoon.FunctorK
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}

package object javaapi {
  type FromTask[A[_]] = FunctionK[Task, A]

  // these two implicit vals below are here just because of usage in Java API (com.avast.clients.rabbitmq.javaapi.RabbitMQJavaConnection)
  private[rabbitmq] implicit def fkToFuture(implicit ec: ExecutionContext): FromTask[Future] = new FunctionK[Task, Future] {
    override def apply[A](fa: Task[A]): Future[A] = fa.runToFuture(Scheduler(ses, ec))
  }

  private[javaapi] implicit def producerFunctorK[A]: FunctorK[ScalaProducer[?[_], A]] = new FunctorK[ScalaProducer[?[_], A]] {
    override def mapK[F[_], G[_]](af: ScalaProducer[F, A])(fToG: ~>[F, G]): ScalaProducer[G, A] =
      (routingKey: String, body: A, properties: Option[ScalaProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  private[javaapi] def pullConsumerToFuture[A: DeliveryConverter](cons: ScalaPullConsumer[Task, A])(
      implicit sch: Scheduler): ScalaPullConsumer[Future, A] = { () =>
    cons
      .pull()
      .map {
        case ScalaPullResult.Ok(deliveryWithHandle) =>
          ScalaPullResult.Ok(new ScalaDeliveryWithHandle[Future, A] {
            override def delivery: ScalaDelivery[A] = deliveryWithHandle.delivery

            override def handle(result: ScalaDeliveryResult): Future[Unit] = deliveryWithHandle.handle(result).runToFuture
          })
        case ScalaPullResult.EmptyQueue => ScalaPullResult.EmptyQueue
      }
      .runToFuture
  }
}
