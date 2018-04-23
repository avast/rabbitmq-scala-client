package com.avast.clients.rabbitmq.javaapi

import java.util.Optional
import java.util.concurrent.CompletableFuture

trait RabbitMQPullConsumer extends AutoCloseable {

  /**
    * Retrieves one message from the queue, if there is any.
    *
    * @return Some(DeliveryHandle[A]) when there was a message available; None otherwise.
    */
  def pull(): CompletableFuture[Optional[DeliveryWithHandle]]
}

trait DeliveryWithHandle {
  def delivery: Delivery

  def handle(result: DeliveryResult): CompletableFuture[Void]
}
