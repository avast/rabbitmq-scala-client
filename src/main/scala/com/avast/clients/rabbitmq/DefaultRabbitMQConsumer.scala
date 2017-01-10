package com.avast.clients.rabbitmq

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQConsumer
import com.avast.kluzo.{Kluzo, TraceId}
import com.avast.metrics.api.Monitor
import com.avast.utils2.JavaConversions._
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.AMQP.Queue.BindOk
import com.rabbitmq.client.{AMQP, DefaultConsumer, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DefaultRabbitMQConsumer(name: String,
                              channel: ServerChannel,
                              useKluzo: Boolean,
                              monitor: Monitor,
                              bindToAction: (String, String) => BindOk)
                             (readAction: Delivery => Future[DeliveryResult])
                             (implicit ec: ExecutionContext)
  extends DefaultConsumer(channel) with RabbitMQConsumer with StrictLogging {

  private val readMeter = monitor.newMeter("read")
  private val processingFailedMeter = monitor.newMeter("processingFailed")

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

          import DeliveryResult._

          readAction(message)
            .andThen {
              case Success(Ack) => ack(messageId, deliveryTag)
              case Success(Reject) => reject(messageId, deliveryTag)
              case Success(Retry) => retry(messageId, deliveryTag)
              case Failure(NonFatal(e)) =>
                processingFailedMeter.mark()
                logger.error("Error while executing callback, it's probably u BUG")
                retry(messageId, deliveryTag)
            }
          ()
        } catch {
          case NonFatal(e) =>
            processingFailedMeter.mark()
            logger.error("Error while executing callback, it's probably u BUG")
            retry(messageId, deliveryTag)
        }
      })
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
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
    }
  }

  private def reject(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT delivery $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, false)
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting the delivery", e)
    }
  }

  private def retry(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] REJECT (with requeue) delivery $messageId, deliveryTag $deliveryTag")
      channel.basicReject(deliveryTag, true)
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while rejecting (with requeue) the delivery", e)
    }
  }
}

case class Delivery(body: Bytes, properties: AMQP.BasicProperties, routingKey: String)
