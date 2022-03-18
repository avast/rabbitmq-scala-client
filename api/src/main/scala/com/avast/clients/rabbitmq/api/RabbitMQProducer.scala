package com.avast.clients.rabbitmq.api

import com.avast.clients.rabbitmq.api.CorrelationIdStrategy.FromPropertiesOrRandomNew

trait RabbitMQProducer[F[_], A] {
  def send(routingKey: String, body: A, properties: Option[MessageProperties] = None)(implicit cidStrategy: CorrelationIdStrategy =
                                                                                        FromPropertiesOrRandomNew(properties)): F[Unit]
}
