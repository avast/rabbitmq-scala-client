package com.avast.clients.rabbitmq.api

trait RabbitMQProducer[F[_], A] {
  def send(routingKey: String, body: A, properties: Option[MessageProperties] = None)(implicit correlationId: CorrelationId =
                                                                                        CorrelationId.createUnsafe): F[Unit]
}
