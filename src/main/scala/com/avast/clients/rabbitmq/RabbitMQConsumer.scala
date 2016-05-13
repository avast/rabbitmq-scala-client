package com.avast.clients.rabbitmq

import java.time.Clock

import com.avast.metrics.api.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Channel, DefaultConsumer, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class RabbitMQConsumer(id: String, channel: Channel, clock: Clock, monitor: Monitor)(readAction: Delivery => Unit)
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
    } finally {
      ack(messageId, deliveryTag)
    }
  }

  private def ack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$id] Confirming delivery $messageId, deliveryTag $deliveryTag")
      channel.basicAck(deliveryTag, true)
    } catch {
      case NonFatal(e) => logger.warn(s"[$id] Error while confirming the delivery", e)
    }
  }
}
