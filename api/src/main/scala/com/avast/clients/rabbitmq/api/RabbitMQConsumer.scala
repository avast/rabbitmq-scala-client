package com.avast.clients.rabbitmq.api

import com.avast.utils2.Done

import scala.util.Try

trait RabbitMQConsumer extends AutoCloseable {
  def bindTo(exchange: String, routingKey: String, arguments: Map[String, Any] = Map.empty): Try[Done]
}
