package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, MessageProperties}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable
import scala.language.higherKinds
import scala.util.control.NonFatal

class MultiFormatConsumer[F[_], A] private (supportedConverters: immutable.Seq[DeliveryConverter[A]],
                                            action: (A, MessageProperties, String) => F[DeliveryResult],
                                            failureAction: (Delivery, ConversionException) => F[DeliveryResult])
    extends (Delivery => F[DeliveryResult])
    with StrictLogging {
  override def apply(delivery: Delivery): F[DeliveryResult] = {
    val converted: Either[ConversionException, A] = try {
      supportedConverters
        .collectFirst {
          case c if c.fits(delivery) => c.convert(delivery)
        }
        .getOrElse {
          Left(ConversionException(s"Could not find suitable converter for $delivery"))
        }
    } catch {
      case NonFatal(e) =>
        logger.debug("Error while converting", e)
        Left(ConversionException("Error while converting", e))
    }

    converted match {
      case Right(cc) => action(cc, delivery.properties, delivery.routingKey)
      case Left(ex: ConversionException) =>
        logger.debug("Could not find suitable converter", ex)
        failureAction(delivery, ex)
      case Left(ex) =>
        logger.debug(s"Error while converting of $delivery", ex)
        failureAction(delivery, ex)
    }
  }
}

object MultiFormatConsumer {
  def forType[F[_], A](supportedConverters: DeliveryConverter[A]*)(
      action: (A, MessageProperties, String) => F[DeliveryResult],
      failureAction: (Delivery, ConversionException) => F[DeliveryResult]): MultiFormatConsumer[F, A] = {
    new MultiFormatConsumer[F, A](supportedConverters.toList, action, failureAction)
  }
}
