package com.avast.clients.rabbitmq.javaapi

import com.avast.clients.rabbitmq.api.{RabbitMQConsumer => ScalaConsumer}

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class RabbitMQConsumer(scalaConsumer: ScalaConsumer) extends AutoCloseable {
  @throws[Exception]
  def bindTo(exchange: String, routingKey: String): Unit = {
    scalaConsumer.bindTo(exchange, routingKey) match {
      case Success(_) => ()
      case Failure(NonFatal(e)) => throw e // thrown intentionally, it's Java API!
    }
  }

  override def close(): Unit = scalaConsumer.close()
}
