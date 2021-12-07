package com.avast.clients.rabbitmq

import cats.effect.Sync
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFunctorOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger

import scala.collection.immutable

class MultiFormatConsumer[F[_], A] private (supportedConverters: immutable.Seq[CheckedDeliveryConverter[A]],
                                            action: Delivery[A] => F[DeliveryResult])(implicit F: Sync[F])
    extends (Delivery[Bytes] => F[DeliveryResult]) {
  private val logger = ImplicitContextLogger.createLogger[F, MultiFormatConsumer[F, A]]

  override def apply(delivery: Delivery[Bytes]): F[DeliveryResult] = {
    implicit val correlationId: CorrelationId = CorrelationId(delivery.properties.correlationId.getOrElse("unknown"))

    F.delay {
        supportedConverters
          .collectFirst {
            case c if c.canConvert(delivery) =>
              delivery.flatMap[A] { d =>
                c.convert(d.body) match {
                  case Right(a) => d.copy(body = a)
                  case Left(ce) => d.toMalformed(ce)
                }
              }
          }
          .getOrElse {
            delivery.toMalformed(ConversionException(s"Could not find suitable converter for $delivery"))
          }
      }
      .recoverWith {
        case e =>
          logger.debug(e)("Error while converting").as {
            delivery.toMalformed(ConversionException("Error while converting", e))
          }
      } >>= action
  }
}

object MultiFormatConsumer {
  def forType[F[_]: Sync, A](supportedConverters: CheckedDeliveryConverter[A]*)(
      action: Delivery[A] => F[DeliveryResult]): MultiFormatConsumer[F, A] = {
    new MultiFormatConsumer[F, A](supportedConverters.toList, action)
  }
}
