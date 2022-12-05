package com.avast.clients.rabbitmq

import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQProducer.SentMessages
import com.avast.clients.rabbitmq.JavaConverters._
import com.avast.clients.rabbitmq.api.CorrelationIdStrategy.FromPropertiesOrRandomNew
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaeffectapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AlreadyClosedException, ConfirmListener, ReturnListener}

import java.util.UUID
import scala.util.control.NonFatal

class DefaultRabbitMQProducer[F[_], A: ProductConverter](name: String,
                                                         exchangeName: String,
                                                         channel: ServerChannel,
                                                         defaultProperties: MessageProperties,
                                                         sentMessages: Option[SentMessages[F]],
                                                         publisherConfirmsConfig: Option[PublisherConfirmsConfig],
                                                         reportUnroutable: Boolean,
                                                         sizeLimitBytes: Option[Int],
                                                         blocker: Blocker,
                                                         logger: ImplicitContextLogger[F],
                                                         monitor: Monitor[F])(implicit F: ConcurrentEffect[F], cs: ContextShift[F])
    extends RabbitMQProducer[F, A] {

  private val sentMeter = monitor.meter("sent")
  private val sentFailedMeter = monitor.meter("sentFailed")
  private val unroutableMeter = monitor.meter("unroutable")
  private val acked = monitor.meter("acked")
  private val nacked = monitor.meter("nacked")

  private val converter = implicitly[ProductConverter[A]]

  private val sendLock = new Object

  channel.addReturnListener(if (reportUnroutable) LoggingReturnListener else NoOpReturnListener)
  publisherConfirmsConfig.foreach(cfg => if (cfg.enabled) {
    channel.confirmSelect()
    channel.addConfirmListener(DefaultConfirmListener)
  })

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
      case Right(convertedBody) =>
        for {
          _ <- checkSize(convertedBody, routingKey)
          result = publisherConfirmsConfig match {
            case Some(cfg @ PublisherConfirmsConfig(true, _)) => sendWithAck(routingKey, convertedBody, finalProperties, 1, cfg)
            case _ => send(routingKey, convertedBody, finalProperties)
          }
          _ <- logErrors(result, routingKey)
        } yield ()
      case Left(ce) => Sync[F].raiseError(ce)
    }
  }

  private def send(routingKey: String, body: Bytes, properties: MessageProperties)(implicit correlationId: CorrelationId): F[Unit] = {
    for {
      _ <- logger.debug(s"Sending message with ${body.size()} B to exchange $exchangeName with routing key '$routingKey' and $properties")
      _ <- blocker.delay {
        sendLock.synchronized {
          // see https://www.rabbitmq.com/api-guide.html#channel-threads
          channel.basicPublish(exchangeName, routingKey, properties.asAMQP, body.toByteArray)
        }
      }
      _ <- sentMeter.mark
    } yield ()
  }

  private def logErrors(from: F[Unit], routingKey: String)(implicit correlationId: CorrelationId): F[Unit] = {
    from.recoverWith {
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

  private def sendWithAck(routingKey: String, body: Bytes, properties: MessageProperties, attemptCount: Int, publisherConfirmsConfig: PublisherConfirmsConfig)(
    implicit correlationId: CorrelationId): F[Unit] = {

    if (attemptCount > publisherConfirmsConfig.sendAttempts) {
      F.raiseError(MaxAttempts("Exhausted max number of attempts"))
    } else {
      val messageId = channel.getNextPublishSeqNo
      for {
        defer <- Deferred.apply[F, Either[Throwable, Unit]]
        _ <- sentMessages.traverse_(_.update(_ + (messageId -> defer)))
        _ <- send(routingKey, body, properties)
        result <- defer.get
        _ <- result match {
          case Left(err) =>
            val sendResult = if (publisherConfirmsConfig.sendAttempts > 1) {
              clearProcessedMessage(messageId) >> sendWithAck(routingKey, body, properties, attemptCount + 1, publisherConfirmsConfig)
            } else {
              F.raiseError(NotAcknowledgedPublish(s"Broker did not acknowledge publish of message $messageId", err))
            }

            nacked.mark >> sendResult // TODO: markovat kazdy nack nebo az pokud se to nepovede?
          case Right(_) =>
            acked.mark >> clearProcessedMessage(messageId)
        }
      } yield ()
    }
  }

  private def clearProcessedMessage(messageId: Long): F[Unit] = {
    sentMessages.traverse_(_.update(_ - messageId))
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

  private object DefaultConfirmListener extends ConfirmListener {
    import cats.syntax.foldable._
    override def handleAck(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Acked $deliveryTag") >> completeDefer(deliveryTag, Right())
      }
    }
    override def handleNack(deliveryTag: Long, multiple: Boolean): Unit = {
      startAndForget {
        logger.plainTrace(s"Not acked $deliveryTag") >> completeDefer(deliveryTag, Left(new Exception(s"Message $deliveryTag not acknowledged by broker")))
      }
    }

    private def completeDefer(deliveryTag: Long, result: Either[Throwable, Unit]): F[Unit] = {
      sentMessages.traverse_(_.get.flatMap(_.get(deliveryTag).traverse_(_.complete(result))))
    }
  }

}

object DefaultRabbitMQProducer {
  type SentMessages[F[_]] = Ref[F, Map[Long, Deferred[F, Either[Throwable, Unit]]]]
}
