package com.avast.clients.rabbitmq.extras

import cats.effect.Effect
import com.avast.clients.rabbitmq.api._
import fs2.Pipe

import scala.language.higherKinds

object StreamingPoisonedMessageHandler {
  import cats.syntax.all._

  def apply[F[_]: Effect, A](maxAttempts: Int)(
      wrappedAction: Delivery[A] => F[DeliveryResult]): Pipe[F, StreamedDelivery[F, A], StreamedResult] = {
    StreamingPoisonedMessageHandler {
      PoisonedMessageHandler[F, A](maxAttempts)(wrappedAction)
    }
  }

  def withCustomPoisonedAction[F[_]: Effect, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])(
      customPoisonedAction: Delivery[A] => F[Unit]): Pipe[F, StreamedDelivery[F, A], StreamedResult] = {
    StreamingPoisonedMessageHandler {
      PoisonedMessageHandler.withCustomPoisonedAction[F, A](maxAttempts)(wrappedAction)(customPoisonedAction)
    }
  }

  private def apply[A, F[_]: Effect](pmh: PoisonedMessageHandler[F, A]): Pipe[F, StreamedDelivery[F, A], StreamedResult] = {
    _.evalMap { d =>
      for {
        realResult <- pmh.apply(d.delivery)
        streamedResult <- d.handle(realResult)
      } yield {
        streamedResult
      }
    }
  }
}
