package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.AMQP


trait RabbitMQSender extends AutoCloseable {
  def send(body: Array[Byte]): Unit

  def send(body: Array[Byte], routingKey: String): Unit

  def send(body: Array[Byte], properties: AMQP.BasicProperties): Unit

  def send(body: Array[Byte], routingKey: String, properties: AMQP.BasicProperties): Unit
}
