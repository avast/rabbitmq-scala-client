package com.avast.clients.rabbitmq.api

trait RabbitMQPullConsumer[F[_], A] {

  /** Retrieves one message from the queue, if there is any.
    */
  def pull(): F[PullResult[F, A]]
}

/** Trait which contains `Delivery` and it's _handle through which it can be *acked*, *rejected* etc.
  */
trait DeliveryWithHandle[+F[_], +A] {
  def delivery: Delivery[A]

  def handle(result: DeliveryResult): F[Unit]
}

sealed trait PullResult[+F[_], +A] {
  def toOption: Option[DeliveryWithHandle[F, A]]
}

object PullResult {

  case class Ok[F[_], A](deliveryWithHandle: DeliveryWithHandle[F, A]) extends PullResult[F, A] {
    override def toOption: Option[DeliveryWithHandle[F, A]] = Some(deliveryWithHandle)
  }

  case object EmptyQueue extends PullResult[Nothing, Nothing] {
    override def toOption: Option[DeliveryWithHandle[Nothing, Nothing]] = None
  }

}
