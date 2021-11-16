package com.avast.clients.rabbitmq.api

trait RabbitMQStreamingConsumer[F[_], A] {
  def deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]]
}

trait StreamedDelivery[F[_], A] {
  def handleWith(f: Delivery[A] => F[DeliveryResult]): F[Unit]
}

trait RabbitMQStreamingConsumerMiddleware[F[_], A] {
  def adjustDelivery(d: StreamedDelivery[F, A]): F[StreamedDelivery[F, A]]
  def adjustResult(rawDelivery: Delivery[A], r: F[DeliveryResult]): F[DeliveryResult]
}
