package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes
import com.rabbitmq.client.AMQP

trait RabbitMQProducer extends AutoCloseable {
  def send(routingKey: String, body: Bytes): Unit

  def send(routingKey: String, body: Bytes, properties: AMQP.BasicProperties): Unit
}
