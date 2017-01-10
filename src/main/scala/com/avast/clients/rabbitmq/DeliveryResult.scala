package com.avast.clients.rabbitmq

sealed trait DeliveryResult

case object Ack extends DeliveryResult
case object Reject extends DeliveryResult
case object Retry extends DeliveryResult
