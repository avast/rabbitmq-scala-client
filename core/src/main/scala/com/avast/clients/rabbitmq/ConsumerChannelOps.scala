package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Timer}
import cats.implicits.catsSyntaxApplicativeError
import cats.syntax.flatMap._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer._
import com.avast.clients.rabbitmq.api._
import com.avast.clients.rabbitmq.logging.ImplicitContextLogger
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties

import scala.jdk.CollectionConverters._
import scala.util._

// it's case-class to have `copy` method for free....
final private[rabbitmq] case class ConsumerChannelOps[F[_]: ConcurrentEffect: Timer: ContextShift, A](
    private val consumerName: String,
    private val queueName: String,
    channel: ServerChannel,
    private val blocker: Blocker,
    republishStrategy: RepublishStrategy[F],
    poisonedMessageHandler: PoisonedMessageHandler[F, A],
    connectionInfo: RabbitMQConnectionInfo,
    private val consumerLogger: ImplicitContextLogger[F],
    private val consumerRootMonitor: Monitor) {

  private val F: ConcurrentEffect[F] = ConcurrentEffect[F] // scalastyle:ignore

  val resultsMonitor: Monitor = consumerRootMonitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")

  def handleResult(messageId: MessageId,
                   deliveryTag: DeliveryTag,
                   properties: BasicProperties,
                   routingKey: RoutingKey,
                   rawBody: Bytes,
                   delivery: Delivery[A])(res: DeliveryResult)(implicit correlationId: CorrelationId): F[Unit] = {
    import DeliveryResult._

    poisonedMessageHandler.interceptResult(delivery, messageId, rawBody)(res).flatMap {
      case Ack => ack(messageId, deliveryTag)
      case Reject => reject(messageId, deliveryTag)
      case Retry => retry(messageId, deliveryTag)
      case Republish(_, newHeaders) =>
        republish(messageId, deliveryTag, createPropertiesForRepublish(newHeaders, properties, routingKey), rawBody)
    }
  }

  protected def ack(messageId: MessageId, deliveryTag: DeliveryTag)(implicit correlationId: CorrelationId): F[Unit] = {
    consumerLogger.debug(s"[$consumerName] ACK delivery $messageId, $deliveryTag") >>
      blocker
        .delay {
          if (!channel.isOpen) throw new IllegalStateException("Cannot ack delivery on closed channel")
          channel.basicAck(deliveryTag.value, false)
          resultAckMeter.mark()
        }
        .attempt
        .flatMap {
          case Right(()) => F.unit
          case Left(e) => consumerLogger.warn(e)(s"[$consumerName] Error while confirming the delivery $messageId")
        }
  }

  protected def reject(messageId: MessageId, deliveryTag: DeliveryTag)(implicit correlationId: CorrelationId): F[Unit] = {
    consumerLogger.debug(s"[$consumerName] REJECT delivery $messageId, $deliveryTag") >>
      blocker
        .delay {
          if (!channel.isOpen) throw new IllegalStateException("Cannot reject delivery on closed channel")
          channel.basicReject(deliveryTag.value, false)
          resultRejectMeter.mark()
        }
        .attempt
        .flatMap {
          case Right(()) => F.unit
          case Left(e) => consumerLogger.warn(e)(s"[$consumerName] Error while rejecting the delivery $messageId")
        }
  }

  protected def retry(messageId: MessageId, deliveryTag: DeliveryTag)(implicit correlationId: CorrelationId): F[Unit] = {
    consumerLogger.debug(s"[$consumerName] REJECT (with requeue) delivery $messageId, $deliveryTag") >>
      blocker
        .delay {
          if (!channel.isOpen) throw new IllegalStateException("Cannot retry delivery on closed channel")
          channel.basicReject(deliveryTag.value, true)
          resultRetryMeter.mark()
        }
        .attempt
        .flatMap {
          case Right(()) => F.unit
          case Left(e) => consumerLogger.warn(e)(s"[$consumerName] Error while rejecting (with requeue) the delivery $messageId")
        }
  }

  protected def republish(messageId: MessageId, deliveryTag: DeliveryTag, properties: BasicProperties, rawBody: Bytes)(
      implicit correlationId: CorrelationId): F[Unit] = {
    republishStrategy
      .republish(blocker, channel, consumerName)(queueName, messageId, deliveryTag, properties, rawBody)
      .flatTap(_ => F.delay(resultRepublishMeter.mark()))
  }

  protected def createPropertiesForRepublish(newHeaders: Map[String, AnyRef],
                                             properties: BasicProperties,
                                             routingKey: RoutingKey): BasicProperties = {
    // values in newHeaders will overwrite values in original headers
    // we must also ensure that UserID will be the same as current username (or nothing): https://www.rabbitmq.com/validated-user-id.html
    val originalUserId = Option(properties.getUserId).filter(_.nonEmpty)
    val h = originalUserId match {
      case Some(uid) => newHeaders + (RepublishOriginalRoutingKeyHeaderName -> routingKey.value) + (RepublishOriginalUserId -> uid)
      case None => newHeaders + (RepublishOriginalRoutingKeyHeaderName -> routingKey.value)
    }
    val headers = Option(properties.getHeaders).map(_.asScala ++ h).getOrElse(h)
    val newUserId = originalUserId match {
      case Some(_) => connectionInfo.username.orNull
      case None => null
    }
    properties.builder().headers(headers.asJava).userId(newUserId).build()
  }
}

class ConsumerChannelOpsFactory[F[_]: ConcurrentEffect: Timer: ContextShift, A: DeliveryConverter](
    consumerName: String,
    queueName: String,
    blocker: Blocker,
    republishStrategy: RepublishStrategy[F],
    poisonedMessageHandler: PoisonedMessageHandler[F, A],
    connectionInfo: RabbitMQConnectionInfo,
    consumerLogger: ImplicitContextLogger[F],
    consumerRootMonitor: Monitor,
    newChannel: Resource[F, ServerChannel]) {

  val create: Resource[F, ConsumerChannelOps[F, A]] = {
    newChannel.map { channel =>
      new ConsumerChannelOps[F, A](consumerName,
                                   queueName,
                                   channel,
                                   blocker,
                                   republishStrategy,
                                   poisonedMessageHandler,
                                   connectionInfo,
                                   consumerLogger,
                                   consumerRootMonitor)
    }
  }
}
