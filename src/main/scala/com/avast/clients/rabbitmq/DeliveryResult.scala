package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.javaapi.{DeliveryResult => JavaResult}

sealed trait DeliveryResult

object DeliveryResult {

  /** Success while processing the message - it will be removed from the queue. */
  case object Ack extends DeliveryResult

  /** Reject the message from processing - it will be removed (discarded). */
  case object Reject extends DeliveryResult

  /** The message cannot be processed but is worth - it will be requeued to the top of the queue. */
  case object Retry extends DeliveryResult

  /** The message cannot be processed but is worth - it will be requeued to the bottom of the queue. */
  case object Republish extends DeliveryResult

  def apply(result: JavaResult): DeliveryResult = {
    result match {
      case JavaResult.Ack    => Ack
      case JavaResult.Reject => Reject
      case JavaResult.Retry  => Retry

      case JavaResult.Republish => Republish
    }
  }
}
