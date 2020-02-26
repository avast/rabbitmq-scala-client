package com.avast.clients.rabbitmq

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.flatMap._
import com.avast.clients.rabbitmq.DefaultRabbitMQConsumer._
import com.avast.clients.rabbitmq.api.DeliveryResult
import com.avast.metrics.scalaapi.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters._
import scala.language.higherKinds
import scala.util.control.NonFatal

private[rabbitmq] trait ConsumerBase[F[_]] extends StrictLogging {
  protected def name: String
  protected def queueName: String
  protected def channel: ServerChannel
  protected def blocker: Blocker
  protected def republishStrategy: RepublishStrategy
  protected implicit def F: Sync[F] // scalastyle:ignore
  protected implicit def cs: ContextShift[F]
  protected def connectionInfo: RabbitMQConnectionInfo
  protected def monitor: Monitor

  protected val resultsMonitor: Monitor = monitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")

  protected def handleResult(messageId: String, deliveryTag: Long, properties: BasicProperties, routingKey: String, body: Array[Byte])(
      res: DeliveryResult): F[Unit] = {
    import DeliveryResult._

    res match {
      case Ack => ack(messageId, deliveryTag)
      case Reject => reject(messageId, deliveryTag)
      case Retry => retry(messageId, deliveryTag)
      case Republish(newHeaders) =>
        republish(messageId, deliveryTag, createPropertiesForRepublish(newHeaders, properties, routingKey), body)
    }

  }

  protected def ack(messageId: String, deliveryTag: Long): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] ACK delivery ID $messageId, deliveryTag $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot ack delivery on closed channel")
        channel.basicAck(deliveryTag, false)
        resultAckMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
      }
    }

  protected def reject(messageId: String, deliveryTag: Long): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] REJECT delivery ID $messageId, deliveryTag $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot reject delivery on closed channel")
        channel.basicReject(deliveryTag, false)
        resultRejectMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
      }
    }

  protected def retry(messageId: String, deliveryTag: Long): F[Unit] =
    blocker.delay {
      try {
        logger.debug(s"[$name] REJECT (with requeue) delivery ID $messageId, deliveryTag $deliveryTag")
        if (!channel.isOpen) throw new IllegalStateException("Cannot retry delivery on closed channel")
        channel.basicReject(deliveryTag, true)
        resultRetryMeter.mark()
      } catch {
        case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
      }
    }

  protected def republish(messageId: String, deliveryTag: Long, properties: BasicProperties, body: Array[Byte]): F[Unit] = {
    republishStrategy
      .republish(blocker, channel, name)(queueName, messageId, deliveryTag, properties, body)
      .flatTap(_ => F.delay(resultRepublishMeter.mark()))
  }

  protected def createPropertiesForRepublish(newHeaders: Map[String, AnyRef],
                                             properties: BasicProperties,
                                             routingKey: String): BasicProperties = {
    // values in newHeaders will overwrite values in original headers
    // we must also ensure that UserID will be the same as current username (or nothing): https://www.rabbitmq.com/validated-user-id.html
    val originalUserId = Option(properties.getUserId).filter(_.nonEmpty)
    val h = originalUserId match {
      case Some(uid) => newHeaders + (RepublishOriginalRoutingKeyHeaderName -> routingKey) + (RepublishOriginalUserId -> uid)
      case None => newHeaders + (RepublishOriginalRoutingKeyHeaderName -> routingKey)
    }
    val headers = Option(properties.getHeaders).map(_.asScala ++ h).getOrElse(h)
    val newUserId = originalUserId match {
      case Some(_) => connectionInfo.username.orNull
      case None => null
    }
    properties.builder().headers(headers.asJava).userId(newUserId).build()
  }
}
