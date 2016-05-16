package com.avast.clients.rabbitmq

import java.time.Clock

import com.avast.metrics.api.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, DefaultConsumer, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class RabbitMQConsumer(id: String, channel: Channel, monitor: Monitor)(readAction: Delivery => Future[Boolean])(implicit ec: ExecutionContext)
  extends DefaultConsumer(channel) with StrictLogging {

  private val readMeter = monitor.newMeter("read")

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val messageId = properties.getMessageId
    val deliveryTag = envelope.getDeliveryTag

    try {
      readMeter.mark()

      logger.debug(s"[$id] Read delivery with ID $messageId, deliveryTag $deliveryTag")

      val message = Delivery(body, properties, Option(envelope.getRoutingKey).getOrElse(""))

      readAction(message)
        .andThen {
          case Success(true) => ack(messageId, deliveryTag)
          case Success(false) => nack(messageId, deliveryTag)
          case Failure(NonFatal(e)) =>
            logger.error("Error while executing callback, it's probably u BUG")
            nack(messageId, deliveryTag)
        }
    } catch {
      case NonFatal(e) =>
        logger.error("Error while executing callback, it's probably u BUG")
        nack(messageId, deliveryTag)
    }
  }

  private def ack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$id] Confirming delivery $messageId, deliveryTag $deliveryTag")
      channel.basicAck(deliveryTag, false)
    } catch {
      case NonFatal(e) => logger.warn(s"[$id] Error while confirming the delivery", e)
    }
  }

  private def nack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$id] Confirming delivery $messageId, deliveryTag $deliveryTag")
      channel.basicNack(deliveryTag, false, true)
    } catch {
      case NonFatal(e) => logger.warn(s"[$id] Error while confirming the delivery", e)
    }
  }
}
