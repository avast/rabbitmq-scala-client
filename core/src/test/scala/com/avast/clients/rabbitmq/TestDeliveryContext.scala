package com.avast.clients.rabbitmq

import com.rabbitmq.client.AMQP.BasicProperties

import scala.util.Random

object TestDeliveryContext {
  private def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString("")
  }

  def create(): DeliveryContext = {
    DeliveryContext(
      messageId = MessageId("test-message-id"),
      correlationId = Some(CorrelationId("test-corr-id")),
      deliveryTag = DeliveryTag(42),
      routingKey = RoutingKey(randomString(10)),
      fixedProperties = new BasicProperties()
    )
  }
}
