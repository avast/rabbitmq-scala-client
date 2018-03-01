package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.api.{Delivery, DeliveryResult, MessageProperties}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.control.NonFatal

class MultiFormatConsumer[A] private (supportedConverters: immutable.Seq[FormatConverter[A]],
                                      action: (A, MessageProperties, String) => Future[DeliveryResult],
                                      failureAction: (Delivery, ConversionException) => Future[DeliveryResult])
    extends (Delivery => Future[DeliveryResult])
    with StrictLogging {
  override def apply(delivery: Delivery): Future[DeliveryResult] = {
    try {
      val converted = supportedConverters
        .collectFirst {
          case c if c.fits(delivery) => c.convert(delivery)
        }
        .getOrElse {
          Left(ConversionException(s"Could not find suitable converter for $delivery"))
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
    } catch {
      case NonFatal(e) =>
        logger.debug("Error while converting", e)
        failureAction(delivery, ConversionException("Error while converting", e))
    }
  }
}

object MultiFormatConsumer {
  def forType[A](supportedConverters: FormatConverter[A]*)(
      action: (A, MessageProperties, String) => Future[DeliveryResult],
      failureAction: (Delivery, ConversionException) => Future[DeliveryResult]): MultiFormatConsumer[A] = {
    new MultiFormatConsumer[A](supportedConverters.toList, action, failureAction)
  }
}

case class ConversionException(desc: String, cause: Throwable = null) extends RuntimeException(desc, cause)
