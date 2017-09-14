package com.avast.clients.rabbitmq.api

import scala.collection.JavaConverters._

sealed trait DeliveryResult

object DeliveryResult {

  /** Success while processing the message - it will be removed from the queue. */
  case object Ack extends DeliveryResult

  /** Reject the message from processing - it will be removed (discarded). */
  case object Reject extends DeliveryResult

  /** The message cannot be processed but is worth - it will be requeued to the top of the queue. */
  case object Retry extends DeliveryResult

  /** The message cannot be processed but is worth - it will be requeued to the bottom of the queue. */
  case class Republish(newHeaders: Map[String, AnyRef] = Map.empty) extends DeliveryResult

}
