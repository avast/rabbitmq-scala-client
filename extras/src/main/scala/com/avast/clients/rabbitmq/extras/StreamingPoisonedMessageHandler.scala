package com.avast.clients.rabbitmq.extras

import cats.effect.Effect
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler.defaultHandlePoisonedMessage
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

  def piped[F[_]: Effect, A](maxAttempts: Int): Pipe[F, StreamedDelivery[F, A], StreamedDelivery[F, A]] = {
    _.map(createStreamedDelivery(_, maxAttempts, defaultHandlePoisonedMessage[F, A](maxAttempts)))
  }

  def pipedWithCustomPoisonedAction[F[_]: Effect, A](maxAttempts: Int)(
      customPoisonedAction: Delivery[A] => F[Unit]): Pipe[F, StreamedDelivery[F, A], StreamedDelivery[F, A]] = {
    _.map(createStreamedDelivery(_, maxAttempts, customPoisonedAction))
  }

  private def createStreamedDelivery[F[_]: Effect, A](d: StreamedDelivery[F, A],
                                                      maxAttempts: Int,
                                                      customPoisonedAction: Delivery[A] => F[Unit]): StreamedDelivery[F, A] = {
    new StreamedDelivery[F, A] {
      override def delivery: Delivery[A] = d.delivery

      override def handle(result: DeliveryResult): F[StreamedResult] = {
        PoisonedMessageHandler.handleResult(d.delivery, maxAttempts, handlePoisonedMessage)(result).flatMap(d.handle)
      }

      private def handlePoisonedMessage(delivery: Delivery[A], ma: Int): F[Unit] = customPoisonedAction(delivery)
    }
  }

  private def apply[F[_]: Effect, A](pmh: PoisonedMessageHandler[F, A]): Pipe[F, StreamedDelivery[F, A], StreamedResult] = {
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
