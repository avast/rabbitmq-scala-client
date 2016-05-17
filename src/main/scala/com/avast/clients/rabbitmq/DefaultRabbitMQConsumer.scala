package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQConsumer
import com.avast.metrics.api.Monitor
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{AMQP, DefaultConsumer, Envelope}
import com.typesafe.scalalogging.StrictLogging

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class DefaultRabbitMQConsumer(name: String,
                              channel: ServerChannel,
                              monitor: Monitor)
                             (readAction: Delivery => Future[Boolean])(implicit ec: ExecutionContext)
  extends DefaultConsumer(channel) with RabbitMQConsumer with StrictLogging {

  private val readMeter = monitor.newMeter("read")

  override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
    val messageId = properties.getMessageId
    val deliveryTag = envelope.getDeliveryTag

    try {
      readMeter.mark()

      logger.debug(s"[$name] Read delivery with ID $messageId, deliveryTag $deliveryTag")

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


  override def close(): Unit = {
    channel.close()
  }

  private def ack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] Confirming delivery $messageId, deliveryTag $deliveryTag")
      channel.basicAck(deliveryTag, false)
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
    }
  }

  private def nack(messageId: String, deliveryTag: Long): Unit = {
    try {
      logger.debug(s"[$name] Confirming delivery $messageId, deliveryTag $deliveryTag")
      channel.basicNack(deliveryTag, false, true)
    } catch {
      case NonFatal(e) => logger.warn(s"[$name] Error while confirming the delivery", e)
    }
  }
}

case class Delivery(body: Array[Byte], properties: AMQP.BasicProperties, routingKey: String)
