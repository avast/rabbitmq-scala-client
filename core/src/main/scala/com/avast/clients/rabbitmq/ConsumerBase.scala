package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.flatMap._
import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer._
import com.avast.clients.rabbitmq.JavaConverters.AmqpPropertiesConversions
import com.avast.clients.rabbitmq.api._
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._
import scala.util._
import scala.util.control.NonFatal

private[rabbitmq] trait ConsumerBase[F[_], A] extends StrictLogging {
  /*
   * This is ugly. However, it needs to be here because this is trait. And it has to be trait, because it's later mixed with `DefaultConsumer`
   * class from Java client...
   *  */
  protected def name: String
  protected def queueName: String
  protected def channel: ServerChannel
  protected def blocker: Blocker
  protected def republishStrategy: RepublishStrategy
  protected def poisonedMessageHandler: PoisonedMessageHandler[F, A]
  protected implicit def F: Sync[F] // scalastyle:ignore
  protected implicit def cs: ContextShift[F]
  protected def connectionInfo: RabbitMQConnectionInfo
  protected def monitor: Monitor
  protected def deliveryConverter: DeliveryConverter[A]

  protected val resultsMonitor: Monitor = monitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")

  protected def parseDelivery(envelope: Envelope, body: Array[Byte], properties: AMQP.BasicProperties): F[DeliveryWithMetadata[A]] = {
    blocker.delay {
      val metadata = DeliveryMetadata.from(envelope, properties)
      val bytes = Bytes.copyFrom(body)

      val delivery = Try(deliveryConverter.convert(Bytes.copyFrom(body))) match {
        case Success(Right(a)) =>
          val delivery = Delivery(a, metadata.fixedProperties.asScala, metadata.routingKey.value)
          logger.trace(s"[$name] Received delivery: ${delivery.copy(body = bytes)}")
          delivery

        case Success(Left(ce)) =>
          val delivery = Delivery.MalformedContent(bytes, metadata.fixedProperties.asScala, metadata.routingKey.value, ce)
          logger.trace(s"[$name] Received delivery but could not convert it: $delivery")
          delivery

        case Failure(ce) =>
          val ex = ConversionException("Unxected failure", ce)
          val delivery = Delivery.MalformedContent(bytes, metadata.fixedProperties.asScala, metadata.routingKey.value, ex)
          logger.trace(s"[$name] Received delivery but could not convert it as the convertor has failed: $delivery")
          delivery
      }

      DeliveryWithMetadata(delivery, metadata)
    }
  }

  protected def handleResult(messageId: MessageId,
                             correlationId: CorrelationId,
                             deliveryTag: DeliveryTag,
                             properties: BasicProperties,
                             routingKey: RoutingKey,
                             body: Array[Byte],
                             delivery: Delivery[A])(res: DeliveryResult): F[Unit] = {
    import DeliveryResult._

    poisonedMessageHandler.interceptResult(delivery, res).flatMap {
      case Ack => ack(messageId, correlationId, deliveryTag)
      case Reject => reject(messageId, correlationId, deliveryTag)
      case Retry => retry(messageId, correlationId, deliveryTag)
      case Republish(_, newHeaders) =>
        republish(messageId, correlationId, deliveryTag, createPropertiesForRepublish(newHeaders, properties, routingKey), body)
    }
  }

  protected def ack(messageId: MessageId, correlationId: CorrelationId, deliveryTag: DeliveryTag): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] ACK delivery $messageId/$correlationId, $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot ack delivery on closed channel")
        channel.basicAck(deliveryTag.value, false)
        resultAckMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery $messageId/$correlationId", e)
      }
    }

  protected def reject(messageId: MessageId, correlationId: CorrelationId, deliveryTag: DeliveryTag): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] REJECT delivery $messageId/$correlationId, $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot reject delivery on closed channel")
        channel.basicReject(deliveryTag.value, false)
        resultRejectMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
      }
    }

  protected def retry(messageId: MessageId, correlationId: CorrelationId, deliveryTag: DeliveryTag): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] REJECT (with requeue) delivery $messageId/$correlationId, $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot retry delivery on closed channel")
        channel.basicReject(deliveryTag.value, true)
        resultRetryMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery $messageId/$correlationId", e)
      }
    }

  protected def republish(messageId: MessageId,
                          correlationId: CorrelationId,
                          deliveryTag: DeliveryTag,
                          properties: BasicProperties,
                          body: Array[Byte]): F[Unit] = {
    republishStrategy
      .republish(blocker, channel, name)(queueName, messageId, correlationId, deliveryTag, properties, body)
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
