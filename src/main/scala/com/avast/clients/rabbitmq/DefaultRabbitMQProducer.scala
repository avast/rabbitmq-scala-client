package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.bytes.Bytes
import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQProducer
import com.avast.metrics.api.Monitor
import com.rabbitmq.client.AMQP
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

class DefaultRabbitMQProducer(name: String,
                              exchangeName: String,
                              channel: ServerChannel, monitor: Monitor) extends RabbitMQProducer with StrictLogging {

  private val sentMeter = monitor.newMeter("sent")
  private val sentFailedMeter = monitor.newMeter("sentFailed")

  override def send(routingKey: String, body: Bytes, properties: AMQP.BasicProperties): Unit = {
    try {
      channel.basicPublish(exchangeName, routingKey, properties, body.toByteArray)
      sentMeter.mark()
    } catch {
      case NonFatal(e) =>
        sentFailedMeter.mark()
        logger.error("Error while sending message", e)
    }
  }

  override def send(routingKey: String, body: Bytes): Unit = {
    val properties = new AMQP.BasicProperties.Builder()
      .messageId(UUID.randomUUID().toString)
      .build()

    send(routingKey, body, properties)
  }

  override def close(): Unit = {
    channel.close()
  }
}
