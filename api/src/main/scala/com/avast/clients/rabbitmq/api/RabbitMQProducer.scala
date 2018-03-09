package com.avast.clients.rabbitmq.api

import com.avast.bytes.Bytes

import scala.language.higherKinds

trait RabbitMQProducer[F[_]] extends AutoCloseable {
  def send(routingKey: String, body: Bytes): F[Unit]

  def send(routingKey: String, body: Bytes, properties: MessageProperties): F[Unit]
}
