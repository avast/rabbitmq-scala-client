package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ContextShift, Effect, Sync}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxFlatMapOps}
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.JavaConverters._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AlreadyClosedException, ReturnListener}

import java.util.UUID
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                         exchangeName: String,
                                                         channel: ServerChannel,
                                                         defaultProperties: MessageProperties,
                                                         reportUnroutable: Boolean,
                                                         blocker: Blocker,
                                                         logger: ImplicitContextLogger[F],
                                                         monitor: Monitor)(implicit F: Effect[F], cs: ContextShift[F])
    extends RabbitMQProducer[F, A] {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)

  override def send(routingKey: String, body: A, properties: Option[MessageProperties] = None)(
      implicit correlationId: CorrelationId = CorrelationId.create(properties)): F[Unit] = {
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
      blocker
        .delay {
          sendLock.synchronized {
            // see https://www.rabbitmq.com/api-guide.html#channel-threads
            channel.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
          }

          // ok!
          sentMeter.mark()
        }
        .recoverWith {
          case ce: AlreadyClosedException =>
            logger.debug(ce)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              F.delay { sentFailedMeter.mark() } >>
              F.raiseError[Unit](ChannelNotRecoveredException("Channel closed, wait for recovery", ce))

          case NonFatal(e) =>
            logger.debug(e)(s"[$name] Failed to send message with routing key '$routingKey' to exchange '$exchangeName'") >>
              F.delay { sentFailedMeter.mark() } >>
              F.raiseError[Unit](e)
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
      F.toIO {
          F.delay { unroutableMeter.mark() } >>
            logger.plainWarn(
              s"[$name] Message sent with routingKey '$routingKey' to exchange '$exchange' (message ID '${properties.getMessageId}', body size ${body.length} B) is unroutable ($replyCode: $replyText)"
            )
        }
        .unsafeToFuture()
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
