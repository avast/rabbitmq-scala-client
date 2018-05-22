package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ConversionException, Delivery, DeliveryResult}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.immutable
import scala.language.higherKinds
import scala.util.control.NonFatal

class MultiFormatConsumer[F[_], A] private (supportedConverters: immutable.Seq[CheckedDeliveryConverter[A]],
                                            action: Delivery[A] => F[DeliveryResult])
    extends (Delivery[Bytes] => F[DeliveryResult])
    with StrictLogging {
  override def apply(delivery: Delivery[Bytes]): F[DeliveryResult] = {
    val converted: Delivery[A] = try {
      supportedConverters
        .collectFirst {
          case c if c.canConvert(delivery) =>
            delivery.flatMap[A] { d =>
              c.convert(d.body) match {
                case Right(a) => d.copy(body = a)
                case Left(ce) =>
                  logger.debug("Error while converting", ce)
                  d.toMalformed(ce)
              }
            }
        }
        .getOrElse {
          delivery.toMalformed(ConversionException(s"Could not find suitable converter for $delivery"))
        }
    } catch {
      case NonFatal(e) =>
        logger.debug("Error while converting", e)
        delivery.toMalformed(ConversionException("Error while converting", e))
    }

    action(converted)
  }
}

object MultiFormatConsumer {
  def forType[F[_], A](supportedConverters: CheckedDeliveryConverter[A]*)(
      action: Delivery[A] => F[DeliveryResult]): MultiFormatConsumer[F, A] = {
    new MultiFormatConsumer[F, A](supportedConverters.toList, action)
  }
}
