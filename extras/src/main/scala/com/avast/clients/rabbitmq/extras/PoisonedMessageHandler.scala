package com.avast.clients.rabbitmq.extras

import cats.Applicative
import cats.effect.Sync
import cats.implicits._
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler.defaultHandlePoisonedMessage
import com.typesafe.scalalogging.StrictLogging

import scala.util.Try
import scala.util.control.NonFatal

trait PoisonedMessageHandler[F[_], A] extends RabbitMQConsumerMiddleware[F, A]

private[rabbitmq] class DefaultPoisonedMessageHandler[F[_], A](maxAttempts: Int)(implicit F: Sync[F])
    extends PoisonedMessageHandler[F, A]
    with StrictLogging {

  override def adjustDelivery(d: Delivery[A]): F[Delivery[A]] = F.pure(d)

  override def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult] = {
    r.flatMap {
      PoisonedMessageHandler.handleResult(rawDelivery, maxAttempts, (d: Delivery[A], _) => handlePoisonedMessage(d))
    }
  }

  /** This method logs the delivery by default but can be overridden. The delivery is always REJECTed after this method execution.
    */
  protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = defaultHandlePoisonedMessage(maxAttempts)(delivery)
}

object PoisonedMessageHandler extends StrictLogging {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  def apply[F[_]: Sync, A](maxAttempts: Int): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts)
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def withCustomPoisonedAction[F[_]: Sync, A](maxAttempts: Int)(
      customPoisonedAction: Delivery[A] => F[Unit]): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts) {
      override protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = customPoisonedAction(delivery)
    }
  }

  private[rabbitmq] def defaultHandlePoisonedMessage[F[_]: Sync, A](maxAttempts: Int)(delivery: Delivery[A]): F[Unit] = Sync[F].delay {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
  }

  private[rabbitmq] def handleResult[F[_]: Sync, A](
      delivery: Delivery[A],
      maxAttempts: Int,
      handlePoisonedMessage: (Delivery[A], Int) => F[Unit])(r: DeliveryResult): F[DeliveryResult] = {
    r match {
      case Republish(newHeaders) => adjustDeliveryResult(delivery, maxAttempts, newHeaders, handlePoisonedMessage)
      case r => Applicative[F].pure(r) // keep other results as they are
    }
  }

  private def adjustDeliveryResult[F[_]: Sync, A](delivery: Delivery[A],
                                                  maxAttempts: Int,
                                                  newHeaders: Map[String, AnyRef],
                                                  handlePoisonedMessage: (Delivery[A], Int) => F[Unit]): F[DeliveryResult] = {
    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programmatically _pretend_ lower attempt number
    val attempt = (delivery.properties.headers ++ newHeaders)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts")

    if (attempt < maxAttempts) {
      Applicative[F].pure(Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
    } else {
      handlePoisonedMessage(delivery, maxAttempts)
        .recover {
          case NonFatal(e) =>
            logger.warn("Custom poisoned message handler failed", e)
            ()
        }
        .map(_ => Reject) // always REJECT the message
    }
  }

}
