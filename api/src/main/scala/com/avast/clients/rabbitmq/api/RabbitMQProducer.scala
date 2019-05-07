package com.avast.clients.rabbitmq.api

import scala.language.higherKinds

trait RabbitMQProducer[F[_], A] {
  def send(routingKey: String, body: A, properties: Option[MessageProperties] = None): F[Unit]
}
