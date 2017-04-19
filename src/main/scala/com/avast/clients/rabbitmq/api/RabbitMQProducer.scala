package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes
import com.avast.utils2.Done
import com.rabbitmq.client.AMQP

import scala.util.Try

trait RabbitMQProducer extends AutoCloseable {
  def send(routingKey: String, body: Bytes): Try[Done]

  def send(routingKey: String, body: Bytes, properties: AMQP.BasicProperties): Try[Done]
}
