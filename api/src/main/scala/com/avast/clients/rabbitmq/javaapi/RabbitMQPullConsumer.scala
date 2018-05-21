package com.avast.clients.rabbitmq.javaapi

import java.util.Optional
import java.util.concurrent.CompletableFuture

trait RabbitMQPullConsumer extends AutoCloseable {

  /**
    * Retrieves one message from the queue, if there is any.
    */
  def pull(): CompletableFuture[PullResult]
}

trait DeliveryWithHandle {
  def delivery: Delivery

  def handle(result: DeliveryResult): CompletableFuture[Void]
}

sealed trait PullResult {
  def toOptional: Optional[DeliveryWithHandle]

  def isOk: Boolean

  def isEmptyQueue: Boolean
}

object PullResult {

  /* These two are not _case_ intentionally - it pollutes the API for Java users */

  class Ok(deliveryWithHandle: DeliveryWithHandle) extends PullResult {
    def getDeliveryWithHandle: DeliveryWithHandle = deliveryWithHandle

    override val toOptional: Optional[DeliveryWithHandle] = Optional.of(deliveryWithHandle)
    override val isOk: Boolean = true
    override val isEmptyQueue: Boolean = false
  }

  object EmptyQueue extends PullResult {
    override val toOptional: Optional[DeliveryWithHandle] = Optional.empty()
    override val isOk: Boolean = false
    override val isEmptyQueue: Boolean = true
  }

}
