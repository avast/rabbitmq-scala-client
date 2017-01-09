package com.avast.clients.rabbitmq

sealed trait DeliveryResult

case object Acknowledge extends DeliveryResult
case object Reject extends DeliveryResult
case object Retry extends DeliveryResult
