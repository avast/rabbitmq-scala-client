package com.avast.clients.rabbitmq.extras

import cats.effect.Effect
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler.defaultHandlePoisonedMessage

trait StreamingPoisonedMessageHandler[F[_], A] extends RabbitMQStreamingConsumerMiddleware[F, A]

object StreamingPoisonedMessageHandler {
  import cats.syntax.all._

  def apply[F[_], A](maxAttempts: Int)(implicit F: Effect[F]): StreamingPoisonedMessageHandler[F, A] = {
    new StreamingPoisonedMessageHandler[F, A] {
      override def adjustDelivery(d: StreamedDelivery[F, A]): F[StreamedDelivery[F, A]] = F.pure(d)

      override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = {
        r.flatMap {
          PoisonedMessageHandler.handleResult(rawDelivery,
                                              maxAttempts,
                                              (d: Delivery[A], _) => defaultHandlePoisonedMessage[F, A](maxAttempts)(d))
        }
      }
    }
  }

  def withCustomPoisonedAction[F[_], A](maxAttempts: Int)(customPoisonedAction: Delivery[A] => F[Unit])(
      implicit F: Effect[F]): StreamingPoisonedMessageHandler[F, A] = {
    new StreamingPoisonedMessageHandler[F, A] {
      override def adjustDelivery(d: StreamedDelivery[F, A]): F[StreamedDelivery[F, A]] = F.pure(d)

      override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = {
        r.flatMap {
          PoisonedMessageHandler.handleResult(rawDelivery, maxAttempts, (d: Delivery[A], _) => customPoisonedAction(d))
        }
      }
    }
  }
}
