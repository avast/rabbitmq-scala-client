package com.avast.clients.rabbitmq.api

import com.rabbitmq.client.AMQP.Queue.BindOk

import scala.util.Try

trait RabbitMQConsumer extends AutoCloseable {
  def bindTo(exchange: String, routingKey: String): Try[BindOk]
}
