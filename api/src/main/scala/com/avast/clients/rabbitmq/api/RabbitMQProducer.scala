package com.avast.clients.rabbitmq.api

import mainecoon.autoFunctorK

import scala.language.higherKinds

@autoFunctorK(autoDerivation = false)
trait RabbitMQProducer[F[_], A] {
  def send(routingKey: String, body: A, properties: Option[MessageProperties] = None): F[Unit]
}
