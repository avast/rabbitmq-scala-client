package com.avast.clients.rabbitmq

import cats.arrow.FunctionK
import cats.effect.IO
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

import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}

package object javaapi {
  type FromIO[A[_]] = IO ~> A

  // these two implicit vals below are here just because of usage in Java API (com.avast.clients.rabbitmq.javaapi.RabbitMQJavaConnection)
  private[rabbitmq] implicit def fkToFuture(implicit ec: ExecutionContext): FromIO[Future] = new FunctionK[IO, Future] {
    override def apply[A](fa: IO[A]): Future[A] = fa.unsafeToFuture()
  }

  private[javaapi] implicit def producerFunctorK[A]: FunctorK[ScalaProducer[*[_], A]] = new FunctorK[ScalaProducer[*[_], A]] {
    override def mapK[F[_], G[_]](af: ScalaProducer[F, A])(fToG: ~>[F, G]): ScalaProducer[G, A] =
      (routingKey: String, body: A, properties: Option[ScalaProperties]) => {
        fToG {
          af.send(routingKey, body, properties)
        }
      }
  }

  private[javaapi] def pullConsumerToFuture[A: DeliveryConverter](cons: ScalaPullConsumer[IO, A]): ScalaPullConsumer[Future, A] = { () =>
    cons
      .pull()
      .map {
        case ScalaPullResult.Ok(deliveryWithHandle) =>
          ScalaPullResult.Ok(new ScalaDeliveryWithHandle[Future, A] {
            override def delivery: ScalaDelivery[A] = deliveryWithHandle.delivery

            override def handle(result: ScalaDeliveryResult): Future[Unit] = deliveryWithHandle.handle(result).unsafeToFuture()
          })
        case ScalaPullResult.EmptyQueue => ScalaPullResult.EmptyQueue
      }
      .unsafeToFuture()
  }
}
