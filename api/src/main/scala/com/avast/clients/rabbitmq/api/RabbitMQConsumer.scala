package com.avast.clients.rabbitmq.api

trait RabbitMQConsumer[F[_]]

trait RabbitMQConsumerMiddleware[F[_], A] {
  def adjustDelivery(d: Delivery[A]): F[Delivery[A]]
  def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult]
}
