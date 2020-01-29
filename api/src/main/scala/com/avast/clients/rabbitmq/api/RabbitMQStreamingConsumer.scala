package com.avast.clients.rabbitmq.api

import scala.language.higherKinds

trait RabbitMQStreamingConsumer[F[_], A] {
  def deliveryStream: fs2.Stream[F, StreamedDelivery[F, A]]
}

trait StreamedDelivery[+F[_], +A] {
  def delivery: Delivery[A]

  def handle(result: DeliveryResult): F[StreamedResult]
}

sealed trait StreamedResult
object StreamedResult extends StreamedResult
