package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes

import scala.language.higherKinds

trait RabbitMQProducer[F[_]] {
  def send(routingKey: String, body: Bytes, properties: Option[MessageProperties] = None): F[Unit]
}
