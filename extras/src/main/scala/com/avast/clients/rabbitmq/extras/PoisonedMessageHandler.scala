package com.avast.clients.rabbitmq.extras

import cats.effect.Sync
import cats.implicits._
import com.avast.clients.rabbitmq.api.DeliveryResult.{Reject, Republish}
import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult}
import com.avast.clients.rabbitmq.extras.PoisonedMessageHandler._
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

trait PoisonedMessageHandler[F[_], A] extends (Delivery[A] => F[DeliveryResult])

private[rabbitmq] class DefaultPoisonedMessageHandler[F[_]: Sync, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])
    extends PoisonedMessageHandler[F, A]
    with StrictLogging {

  private val F = implicitly[Sync[F]]

  override def apply(delivery: Delivery[A]): F[DeliveryResult] = {
    wrappedAction(delivery)
      .flatMap {
        case Republish(newHeaders) => republishDelivery(delivery, newHeaders)
        case r => F.pure(r) // keep other results as they are
      }
  }

  private def republishDelivery(delivery: Delivery[A], newHeaders: Map[String, AnyRef]): F[DeliveryResult] = {
    // get current attempt no. from passed headers with fallback to original (incoming) headers - the fallback will most likely happen
    // but we're giving the programmer chance to programatically _pretend_ lower attempt number
    val attempt = (newHeaders ++ delivery.properties.headers)
      .get(RepublishCountHeaderName)
      .flatMap(v => Try(v.toString.toInt).toOption)
      .getOrElse(0) + 1

    logger.debug(s"Attempt $attempt/$maxAttempts")

    if (attempt < maxAttempts) {
      F.pure(Republish(newHeaders + (RepublishCountHeaderName -> attempt.asInstanceOf[AnyRef])))
    } else {
      handlePoisonedMessage(delivery)
        .recover {
          case NonFatal(e) =>
            logger.warn("Custom poisoned message handler failed", e)
            ()
        }
        .map(_ => Reject) // always REJECT the message
    }
  }

  /** This method logs the delivery by default but can be overridden. The delivery is always REJECTed after this method execution.
    */
  protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = F.delay {
    logger.warn(s"Message failures reached the limit $maxAttempts attempts, throwing away: $delivery")
  }
}

object PoisonedMessageHandler {
  final val RepublishCountHeaderName: String = "X-Republish-Count"

  def apply[F[_]: Sync, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult]): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts)(wrappedAction)
  }

  /**
    * @param customPoisonedAction The delivery is always REJECTed after this method execution.
    */
  def withCustomPoisonedAction[F[_]: Sync, A](maxAttempts: Int)(wrappedAction: Delivery[A] => F[DeliveryResult])(
      customPoisonedAction: Delivery[A] => F[Unit]): PoisonedMessageHandler[F, A] = {
    new DefaultPoisonedMessageHandler[F, A](maxAttempts)(wrappedAction) {
      override protected def handlePoisonedMessage(delivery: Delivery[A]): F[Unit] = customPoisonedAction(delivery)
    }
  }
}
