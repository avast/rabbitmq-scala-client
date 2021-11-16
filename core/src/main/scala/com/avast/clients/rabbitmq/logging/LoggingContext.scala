package com.avast.clients.rabbitmq.logging

private[rabbitmq] trait LoggingContext {
  def asContextMap: Map[String, String]
}
