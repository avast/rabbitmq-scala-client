package com.avast.clients.rabbitmq

import java.util.UUID

import cats.effect.{Blocker, ContextShift, Effect, Sync}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.api.{ChannelNotRecoveredException, MessageProperties, RabbitMQProducer}
import com.avast.clients.rabbitmq.javaapi.JavaConverters._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AlreadyClosedException, ReturnListener}
import com.typesafe.scalalogging.StrictLogging

import scala.language.higherKinds
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_]: Sync, A: ProductConverter](name: String,
                                                               exchangeName: String,
                                                               channel: ServerChannel,
                                                               defaultProperties: MessageProperties,
                                                               reportUnroutable: Boolean,
                                                               blocker: Blocker,
                                                               monitor: Monitor)(implicit F: Effect[F], cs: ContextShift[F])
    extends RabbitMQProducer[F, A]
    with StrictLogging {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)

  override def send(routingKey: String, body: A, properties: Option[MessageProperties] = None): F[Unit] = {
    val finalProperties = {
      val initialProperties = properties.getOrElse(defaultProperties.copy(messageId = Some(UUID.randomUUID().toString)))
      converter.fillProperties(initialProperties)
    }

    converter.convert(body) match {
      case Right(convertedBody) => send(routingKey, convertedBody, finalProperties)
      case Left(ce) => Sync[F].raiseError(ce)
    }
  }

  private def send(routingKey: String, body: Bytes, properties: MessageProperties): F[Unit] = {
    blocker.delay {
      logger.debug(s"Sending message with ${body.size()} B to exchange $exchangeName with routing key '$routingKey' and $properties")

      try {
        sendLock.synchronized {
          // see https://www.rabbitmq.com/api-guide.html#channel-threads
          channel.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
        }

        // ok!
        sentMeter.mark()
      } catch {
        case ce: AlreadyClosedException =>
          logger.debug(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'", ce)
          sentFailedMeter.mark()
          throw ChannelNotRecoveredException("Channel closed, wait for recovery", ce)

        case NonFatal(e) =>
          logger.debug(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'", e)
          sentFailedMeter.mark()
          throw e
      }
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
      unroutableMeter.mark()
      logger.warn(
        s"[$name] Message sent with routingKey '$routingKey' to exchange '$exchange' (message ID '${properties.getMessageId}', body size ${body.length} B) is unroutable ($replyCode: $replyText)"
      )
    }
  }

  private object NoOpReturnListener extends ReturnListener {
    override def handleReturn(replyCode: Int,
                              replyText: String,
                              exchange: String,
                              routingKey: String,
                              properties: BasicProperties,
                              body: Array[Byte]): Unit = {
      unroutableMeter.mark()
    }
  }

}
