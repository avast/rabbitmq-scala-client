package com.avast.clients.rabbitmq.javaapi

import com.avast.clients.rabbitmq.api.{RabbitMQConsumer => ScalaConsumer}

class DefaultRabbitMQConsumer(scalaConsumer: ScalaConsumer with AutoCloseable) extends RabbitMQConsumer {
  override def close(): Unit = scalaConsumer.close()
}
