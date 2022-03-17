package com.avast.clients.rabbitmq

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Timer}
import cats.implicits.{catsSyntaxApplicativeError, toFunctorOps}
import cats.syntax.flatMap._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.JavaConverters.AmqpPropertiesConversions
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.{AMQP, Envelope}
import org.slf4j.event.Level

import scala.concurrent.TimeoutException
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util._

// it's case-class to have `copy` method for free....
final private[rabbitmq] case class ConsumerBase[F[_]: ConcurrentEffect: Timer, A](
    consumerName: String,
    queueName: String,
    blocker: Blocker,
    consumerLogger: ImplicitContextLogger[F],
    consumerRootMonitor: Monitor[F])(implicit val contextShift: ContextShift[F], implicit val deliveryConverter: DeliveryConverter[A]) {

  val F: ConcurrentEffect[F] = ConcurrentEffect[F] // scalastyle:ignore

  private val timeoutsMeter = consumerRootMonitor.meter("timeouts")

  def parseDelivery(envelope: Envelope, rawBody: Bytes, properties: AMQP.BasicProperties): F[DeliveryWithContext[A]] = {
    implicit val dctx: DeliveryContext = DeliveryContext.from(envelope, properties)
    import dctx.fixedProperties

    blocker
      .delay(Try(deliveryConverter.convert(rawBody)))
      .flatMap[Delivery[A]] {
        case Success(Right(a)) =>
          val delivery = Delivery(a, fixedProperties.asScala, dctx.routingKey.value)

          consumerLogger.trace(s"[$consumerName] Received delivery from queue '$queueName': ${delivery.copy(body = rawBody)}").as {
            delivery
          }

        case Success(Left(ce)) =>
          val delivery = Delivery.MalformedContent(rawBody, fixedProperties.asScala, dctx.routingKey.value, ce)

          consumerLogger.trace(s"[$consumerName] Received delivery from queue '$queueName' but could not convert it: $delivery").as {
            delivery
          }

        case Failure(ce) =>
          val ex = ConversionException("Unexpected failure", ce)
          val delivery = Delivery.MalformedContent(rawBody, fixedProperties.asScala, dctx.routingKey.value, ex)

          consumerLogger
            .trace(
              s"[$consumerName] Received delivery from queue '$queueName' but could not convert it as the convertor has failed: $delivery")
            .as(delivery)
      }
      .map(DeliveryWithContext(_, dctx))
  }

  def watchForTimeoutIfConfigured(processTimeout: FiniteDuration, timeoutAction: DeliveryResult, timeoutLogLevel: Level)(
      delivery: Delivery[A],
      result: F[ConfirmedDeliveryResult[F]])(
      customTimeoutAction: F[Unit],
  )(implicit dctx: DeliveryContext): F[ConfirmedDeliveryResult[F]] = {
    import dctx._

    if (processTimeout != Duration.Zero) {
      Concurrent
        .timeout(result, processTimeout)
        .recoverWith {
          case e: TimeoutException =>
            customTimeoutAction >>
              consumerLogger.trace(e)(s"[$consumerName] Timeout for $messageId") >>
              timeoutsMeter.mark >> {

              lazy val msg =
                s"[$consumerName] Task timed-out after $processTimeout of processing delivery $messageId with routing key ${delivery.routingKey}, applying DeliveryResult.$timeoutAction. Delivery was:\n$delivery"

              (timeoutLogLevel match {
                case Level.ERROR => consumerLogger.error(msg)
                case Level.WARN => consumerLogger.warn(msg)
                case Level.INFO => consumerLogger.info(msg)
                case Level.DEBUG => consumerLogger.debug(msg)
                case Level.TRACE => consumerLogger.trace(msg)
              }).as {
                ConfirmedDeliveryResult[F](timeoutAction)
              }
            }
        }
    } else result
  }
}
