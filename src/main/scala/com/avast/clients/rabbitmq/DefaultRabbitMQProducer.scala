package com.avast.clients.rabbitmq

import java.util.UUID

import com.avast.clients.rabbitmq.RabbitMQChannelFactory.ServerChannel
import com.avast.clients.rabbitmq.api.RabbitMQProducer
import com.avast.metrics.api.Monitor
import com.rabbitmq.client.AMQP
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

class DefaultRabbitMQProducer(name: String,
                              exchangeName: String,
                              channel: ServerChannel, monitor: Monitor) extends RabbitMQProducer with StrictLogging {

  override def send(routingKey: String, body: Array[Byte], properties: AMQP.BasicProperties): Unit = {
    try {
      channel.basicPublish(exchangeName, routingKey, properties, body)
    } catch {
      case NonFatal(e) => logger.error("Error while sending message", e)
    }
  }

  override def send(routingKey: String, body: Array[Byte]): Unit = {
    val properties = new AMQP.BasicProperties.Builder()
      .messageId(UUID.randomUUID().toString)
      .build()

    send(routingKey, body, properties)
  }

  override def close(): Unit = {
    channel.close()
  }
}
