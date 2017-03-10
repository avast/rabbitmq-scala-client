package com.avast.clients.rabbitmq

import com.avast.clients.rabbitmq.javaapi.{DeliveryResult => JavaResult}

sealed trait DeliveryResult

object DeliveryResult {

  case object Ack extends DeliveryResult

  case object Reject extends DeliveryResult

  case object Retry extends DeliveryResult

  def apply(result: JavaResult): DeliveryResult = {
    result match {
      case JavaResult.Ack => Ack
      case JavaResult.Reject => Reject
      case JavaResult.Retry => Retry
    }
  }
}
