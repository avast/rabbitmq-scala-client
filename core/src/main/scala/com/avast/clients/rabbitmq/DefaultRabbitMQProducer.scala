package com.avast.clients.rabbitmq

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ContextShift, Effect, Sync}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFlatMapOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.JavaConverters._
import com.avast.clients.rabbitmq.api.CorrelationIdStrategy.FromPropertiesOrRandomNew
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.AlreadyClosedException

import java.util.UUID
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                         exchangeName: String,
                                                         channel: RecoverableChannel[F],
                                                         defaultProperties: MessageProperties,
                                                         blocker: Blocker,
                                                         logger: ImplicitContextLogger[F],
                                                         monitor: Monitor[F])(implicit F: Effect[F], cs: ContextShift[F])
    extends RabbitMQProducer[F, A] {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  override def send(routingKey: String, body: A, properties: Option[MessageProperties] = None)(
      implicit cidStrategy: CorrelationIdStrategy = FromPropertiesOrRandomNew(properties)): F[Unit] = {
    implicit val correlationId: CorrelationId = CorrelationId(cidStrategy.toCIDValue)

    val finalProperties = converter.fillProperties {
      val initialProperties = properties.getOrElse(defaultProperties)

      initialProperties.copy(
        messageId = initialProperties.messageId.orElse(Some(UUID.randomUUID().toString)),
        correlationId = Some(correlationId.value) // it was taken from it if it was there
      )
    }

    converter.convert(body) match {
      case Right(convertedBody) => send(routingKey, convertedBody, finalProperties)
      case Left(ce) => Sync[F].raiseError(ce)
    }
  }

  private def send(routingKey: String, body: Bytes, properties: MessageProperties)(implicit correlationId: CorrelationId): F[Unit] = {
    logger.debug(s"Sending message with ${body.size()} B to exchange $exchangeName with routing key '$routingKey' and $properties") >>
      channel.get
        .flatMap { ch =>
          blocker.delay {
            sendLock.synchronized {
              // see https://www.rabbitmq.com/api-guide.html#channel-threads
              ch.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
            }
          }
        }
        .flatTap(_ => sentMeter.mark)
        .recoverWith {
          case ce: AlreadyClosedException =>
            logger.debug(ce)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              sentFailedMeter.mark >>
              channel.recover >>
              F.raiseError[Unit](ChannelNotRecoveredException("Channel closed, wait for recovery", ce))

          case NonFatal(e) =>
            logger.debug(e)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              sentFailedMeter.mark >>
              channel.recover >>
              F.raiseError[Unit](e)
        }
  }

  private[rabbitmq] def close(): F[Unit] = ???
}
