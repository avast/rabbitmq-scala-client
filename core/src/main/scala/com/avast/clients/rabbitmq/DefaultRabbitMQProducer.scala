package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ContextShift, Effect, Sync}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps, toFlatMapOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.JavaConverters._
import com.avast.clients.rabbitmq.api.CorrelationIdStrategy.FromPropertiesOrRandomNew
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AlreadyClosedException, ReturnListener}

import java.util.UUID
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                         exchangeName: String,
                                                         channel: ServerChannel,
                                                         defaultProperties: MessageProperties,
                                                         reportUnroutable: Boolean,
                                                         sizeLimitBytes: Option[Int],
                                                         blocker: Blocker,
                                                         logger: ImplicitContextLogger[F],
                                                         monitor: Monitor[F])(implicit F: Effect[F], cs: ContextShift[F])
    extends RabbitMQProducer[F, A] {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)

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
    checkSize(body, routingKey) >>
      logger.debug(s"Sending message with ${body.size()} B to exchange $exchangeName with routing key '$routingKey' and $properties") >>
      blocker
        .delay {
          sendLock.synchronized {
            // see https://www.rabbitmq.com/api-guide.html#channel-threads
            channel.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
          }
        }
        .flatTap(_ => sentMeter.mark)
        .recoverWith {
          case ce: AlreadyClosedException =>
            logger.debug(ce)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              sentFailedMeter.mark >>
              F.raiseError[Unit](ChannelNotRecoveredException("Channel closed, wait for recovery", ce))

          case NonFatal(e) =>
            logger.debug(e)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              sentFailedMeter.mark >>
              F.raiseError[Unit](e)
        }
  }

  private def checkSize(bytes: Bytes, routingKey: String)(implicit correlationId: CorrelationId): F[Unit] = {
    sizeLimitBytes match {
      case Some(limit) =>
        val size = bytes.size()
        if (size >= limit) {
          logger.warn {
            s"[$name] Will not send message with $size B to exchange $exchangeName with routing key '$routingKey' as it is over the limit $limit B"
          } >> F.raiseError[Unit](TooBigMessage(s"Message too big ($size/$limit)"))
        } else F.unit

      case None => F.unit
    }
  }

  // scalastyle:off
  private object LoggingReturnListener extends ReturnListener {
    override def handleReturn(replyCode: Int,
                              replyText: String,
                              exchange: String,
                              routingKey: String,
                              properties: BasicProperties,
                              body: Array[Byte]): Unit = {
      startAndForget {
        unroutableMeter.mark >>
          logger.plainWarn(
            s"[$name] Message sent with routingKey '$routingKey' to exchange '$exchange' (message ID '${properties.getMessageId}', body size ${body.length} B) is unroutable ($replyCode: $replyText)"
          )
      }
    }
  }

  private object NoOpReturnListener extends ReturnListener {
    override def handleReturn(replyCode: Int,
                              replyText: String,
                              exchange: String,
                              routingKey: String,
                              properties: BasicProperties,
                              body: Array[Byte]): Unit = {
      startAndForget {
        unroutableMeter.mark
      }
    }
  }

}
