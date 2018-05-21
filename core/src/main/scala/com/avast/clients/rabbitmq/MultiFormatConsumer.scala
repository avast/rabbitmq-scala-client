package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery, DeliveryResult}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable
import scala.language.higherKinds
import scala.util.control.NonFatal

class MultiFormatConsumer[F[_], A] private (supportedConverters: immutable.Seq[CheckedDeliveryConverter[A]],
                                            action: Delivery[A] => F[DeliveryResult],
                                            failureAction: (Delivery[Bytes], ConversionException) => F[DeliveryResult])
    extends (Delivery[Bytes] => F[DeliveryResult])
    with StrictLogging {
  override def apply(delivery: Delivery[Bytes]): F[DeliveryResult] = {
    val converted: Either[ConversionException, Delivery[A]] = try {
      supportedConverters
        .collectFirst {
          case c if c.canConvert(delivery) =>
            c.convert(delivery.body).map { newBody =>
              delivery.copy(body = newBody)
            }
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
      case Right(convertedDelivery) => action(convertedDelivery)
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
  def forType[F[_], A](supportedConverters: CheckedDeliveryConverter[A]*)(
      action: Delivery[A] => F[DeliveryResult],
      failureAction: (Delivery[Bytes], ConversionException) => F[DeliveryResult]): MultiFormatConsumer[F, A] = {
    new MultiFormatConsumer[F, A](supportedConverters.toList, action, failureAction)
  }
}
