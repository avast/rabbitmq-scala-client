package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQConsumer
import com.avast.kluzo.{Kluzo, TraceId}
import com.avast.metrics.scalaapi.Monitor
import com.avast.utils2.JavaConversions._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.AMQP.Queue.BindOk
import com.rabbitmq.client.{AMQP, DefaultConsumer, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DefaultRabbitMQConsumer(
    name: String,
    channel: ServerChannel,
    queueName: String,
    useKluzo: Boolean,
    monitor: Monitor,
    failureAction: DeliveryResult,
    bindToAction: (String, String) => BindOk)(readAction: Delivery => Future[DeliveryResult])(implicit ec: ExecutionContext)
    extends DefaultConsumer(channel)
    with RabbitMQConsumer
    with StrictLogging {

  private val readMeter = monitor.meter("read")
  private val resultsMonitor = monitor.named("results")
  private val resultAckMeter = resultsMonitor.meter("ack")
  private val resultRejectMeter = resultsMonitor.meter("reject")
  private val resultRetryMeter = resultsMonitor.meter("retry")
  private val resultRepublishMeter = resultsMonitor.meter("republish")
  private val processingFailedMeter = resultsMonitor.meter("processingFailed")

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val traceId = if (useKluzo && properties.getHeaders != null) {
      val traceId = Option(properties.getHeaders.get(Kluzo.HttpHeaderName))
        .map(_.toString)
        .map(TraceId(_))
        .getOrElse(TraceId.generate)

      Some(traceId)
    } else {
      None
    }

    Kluzo.withTraceId(traceId) {
      ec.execute(() => {
        val messageId = properties.getMessageId
        val deliveryTag = envelope.getDeliveryTag

        try {
          readMeter.mark()

          logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

          val message = Delivery(Bytes.copyFrom(body), properties, Option(envelope.getRoutingKey).getOrElse(""))

          readAction(message).andThen(handleResult(messageId, deliveryTag, properties, body))

          ()
        } catch {
          case NonFatal(e) =>
            processingFailedMeter.mark()
            logger.error("Error while executing callback, it's probably u BUG", e)
            retry(messageId, deliveryTag)
        }
      })
    }
  }

  private def handleResult(messageId: String,
                           deliveryTag: Long,
                           properties: BasicProperties,
                           body: Array[Byte]): PartialFunction[Try[DeliveryResult], Unit] = {
    import DeliveryResult._

    {
      case Success(Ack) => ack(messageId, deliveryTag)
      case Success(Reject) => reject(messageId, deliveryTag)
      case Success(Retry) => retry(messageId, deliveryTag)
      case Success(Republish) => republish(messageId, deliveryTag, properties, body)
      case Failure(NonFatal(e)) =>
        processingFailedMeter.mark()
        logger.error("Error while executing callback, it's probably a BUG")

        failureAction match {
          case (Ack) => ack(messageId, deliveryTag)
          case (Reject) => reject(messageId, deliveryTag)
          case (Retry) => retry(messageId, deliveryTag)
          case (Republish) => republish(messageId, deliveryTag, properties, body)
        }
    }
  }

  override def bindTo(exchange: String, routingKey: String): Try[BindOk] = Try {
    bindToAction(exchange, routingKey)
  }

  override def close(): Unit = {
    channel.close()
  }

  private def ack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] ACK delivery $messageId, deliveryTag $deliveryTag")
      channel.basicAck(deliveryTag, false)
      resultAckMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
    }
  }

  private def reject(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT delivery $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, false)
      resultRejectMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
    }
  }

  private def retry(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT (with requeue) delivery $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, true)
      resultRetryMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
    }
  }

  private def republish(messageId: String, deliveryTag: Long, properties: BasicProperties, body: Array[Byte]): Unit = {
    try {
      logger.debug(s"[$name] Republishing delivery ($messageId, deliveryTag $deliveryTag) to end of queue '$queueName'")
      channel.basicPublish("", queueName, properties, body)
      channel.basicAck(deliveryTag, false)
      resultRepublishMeter.mark()
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
    }
  }
}

case class Delivery(body: Bytes, properties: AMQP.BasicProperties, routingKey: String)
