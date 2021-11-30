package com.avast.clients.rabbitmq.api

trait RabbitMQStreamingConsumer[F[_], A] {
  def deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]]
}

trait StreamedDelivery[F[_], A] {
  def handleWith(f: Delivery[A] => F[DeliveryResult]): F[Unit]
}
