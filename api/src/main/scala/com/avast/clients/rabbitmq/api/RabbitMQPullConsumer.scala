package com.avast.clients.rabbitmq.api

import scala.language.higherKinds

trait RabbitMQPullConsumer[F[_], A] {

  /** Retrieves one message from the queue, if there is any.
    *
    * @return Some(DeliveryHandle[A]) when there was a message available; None otherwise.
    */
  def pull(): F[Option[DeliveryWithHandle[F, A]]]
}

/** Trait which contains `Delivery` and it's _handle through which it can be *acked*, *rejected* etc.
  */
trait DeliveryWithHandle[F[_], A] {
  def delivery: Delivery[A]

  def handle(result: DeliveryResult): F[Unit]
}
