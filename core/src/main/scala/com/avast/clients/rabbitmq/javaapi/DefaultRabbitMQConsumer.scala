package com.avast.clients.rabbitmq.javaapi

import com.avast.clients.rabbitmq.api.{RabbitMQConsumer => ScalaConsumer}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class DefaultRabbitMQConsumer(scalaConsumer: ScalaConsumer[Future]) extends RabbitMQConsumer {
  override def close(): Unit = Await.result(scalaConsumer.close(), Duration.Inf)
}
